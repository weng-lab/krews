package krews.core

import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.*
import krews.misc.createReport
import mu.KotlinLogging
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean


private val log = KotlinLogging.logger {}

const val DB_SNAPSHOT_FILENAME = "$DB_FILENAME.snapshot"

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val executor: LocallyDirectedExecutor,
    runTimestampOverride: Long? = null
) {
    private val db: Database
    private lateinit var workflowRun: WorkflowRun
    private val reportPool: ScheduledExecutorService
    private val dbUploadPool: ScheduledExecutorService
    private val workflowTime = if (runTimestampOverride != null) DateTime(runTimestampOverride) else DateTime.now()
    private val workflowTmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "workflow-${workflow.name}-$workflowTime")
    private val hasShutdown = AtomicBoolean(false)

    init {
        val dbFilePath = workflowTmpDir.resolve(DB_FILENAME)
        executor.downloadFile(DB_FILENAME, dbFilePath)
        db = migrateAndConnectDb(dbFilePath)
        val dbPermissions = PosixFilePermissions.fromString("rwxrwxrwx")
        Files.setPosixFilePermissions(dbFilePath, dbPermissions)
        Files.setPosixFilePermissions(dbFilePath.parent, dbPermissions)

        val dbUploadThreadFactory = BasicThreadFactory.Builder().namingPattern("db-upload-%d").build()
        dbUploadPool = Executors.newSingleThreadScheduledExecutor(dbUploadThreadFactory)

        val reportThreadFactory = BasicThreadFactory.Builder().namingPattern("report-gen-%d").build()
        reportPool = Executors.newSingleThreadScheduledExecutor(reportThreadFactory)
    }

    fun run() {
        transaction(db) {
            WorkflowRuns.deleteAll()
            TaskRuns.deleteAll()
        }

        // Create the workflow run in the database
        log.info { "Creating workflow run for workflow ${workflow.name} with timestamp ${workflowTime.millis}" }
        transaction(db) {
            workflowRun = WorkflowRun.new {
                workflowName = workflow.name
                startTime = workflowTime.millis
            }
        }
        log.info { "Workflow run created successfully!" }

        // Create an executor service for periodically uploading the db file
        val dbUploadDelay = Math.max(workflowConfig.dbUploadDelay, 30)
        dbUploadPool.scheduleWithFixedDelay({ uploadDb() },
                dbUploadDelay, dbUploadDelay, TimeUnit.SECONDS)

        // Create an executor service for periodically generating reports
        val reportGenerationDelay = Math.max(workflowConfig.reportGenerationDelay, 10)
        reportPool.scheduleWithFixedDelay({ generateReport() },
            reportGenerationDelay, reportGenerationDelay, TimeUnit.SECONDS)

        val workflowRunSuccessful = runWorkflow()

        transaction(db) {
            workflowRun.completedSuccessfully = workflowRunSuccessful
            workflowRun.completedTime = DateTime.now().millis
        }

        transaction(db) {
            // if config.cleanOldOutputs delete output files not created by this run
            if (workflowConfig.cleanOldFiles) {
                log.info { "Cleaning input and output files not used by this run..." }

                CachedInputFiles.deleteWhere { CachedInputFiles.latestUseWorkflowRunId neq workflowRun.id.value }
                val cachedInputFiles = CachedInputFile.all().map { it.path }.toSet()
                val inputPathFiles = executor.listFiles(INPUTS_DIR).map { it.substringAfter("$INPUTS_DIR/").replace("\\", "/") }
                for (inputPathFile in inputPathFiles) {
                    if (!cachedInputFiles.contains(inputPathFile)) {
                        val fileToDelete = "$INPUTS_DIR/$inputPathFile"
                        log.info { "Deleting unused input file $fileToDelete" }
                        executor.deleteFile(fileToDelete)
                    }
                }
                CachedOutputs.deleteWhere { CachedOutputs.latestUseWorkflowRunId neq workflowRun.id.value }
                val cachedOutputFiles = CachedOutput.all().flatMap { it.outputFiles.split(",") }.toSet()
                val outputPathFiles = executor.listFiles(OUTPUTS_DIR).map { it.substringAfter("$OUTPUTS_DIR/").replace("\\", "/") }
                for (outputPathFile in outputPathFiles) {
                    if (!cachedOutputFiles.contains(outputPathFile)) {
                        val fileToDelete = "$OUTPUTS_DIR/$outputPathFile"
                        log.info { "Deleting unused output file $fileToDelete" }
                        executor.deleteFile(fileToDelete)
                    }
                }
            }
        }

        onShutdown()
    }

    private fun runWorkflow(): Boolean {
        val taskRunner = TaskRunner(workflowRun, workflowConfig, executor, db)
        try {
            taskRunner.startMonitorTasks()

            // Set execute function for each task.
            for (task in workflow.tasks.values) {
                task.connect(workflowConfig.tasks[task.name], taskRunner)
            }

            // Get "leafOutputs", meaning this workflow's task.output fluxes that don't have other task.outputs as parents
            val allTaskOutputFluxes = workflow.tasks.values.map { it.outputPub }

            // Trigger workflow by subscribing to leaf task outputs...
            val leavesFlux = Flux
                .merge(allTaskOutputFluxes.map {
                    it.onErrorResume { e ->
                        log.error(e) { "Error returned from flux." }
                        Mono.empty()
                    }
                })
                .subscribeOn(Schedulers.elastic())

            // and block until it's done
            leavesFlux.blockLast()

            val failedTasks = transaction(db) {
                TaskRuns.select {
                    TaskRuns.workflowRunId eq workflowRun.id and TaskRuns.completedSuccessfully.eq(false)
                }.count()
            }
            return failedTasks == 0
        } catch(e: Exception) {
            log.error(e) { "Workflow unsuccessful." }
            return false
        } finally {
            taskRunner.stopMonitorTasks()
        }
    }

    fun onShutdown() {
        // TODO this should propagate through TaskRunner and Executor too
        if (!hasShutdown.compareAndSet(false, true)) return
        log.info { "Shutting down..." }
        reportPool.shutdown()
        dbUploadPool.shutdown()
        // Stop the periodic db upload and report generation executor service and generate one final report.
        reportPool.awaitTermination(1000, TimeUnit.SECONDS)
        dbUploadPool.awaitTermination(1000, TimeUnit.SECONDS)

        uploadDb()
        generateReport()

        Files.walk(workflowTmpDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
        log.info { "Shutdown complete!" }
    }

    /*
     * Backs up the sqlite database file and uploads the backup.
     * This is needed because it's not safe to copy a file that's currently open for writes.
     */
    private fun uploadDb() {
        log.info { "Uploading database..." }
        // Create backup db file
        val dbSnapshotPath = workflowTmpDir.resolve(DB_SNAPSHOT_FILENAME)
        log.info { "Backing up database..." }
        transaction(db) {
            TransactionManager.current().connection.createStatement().use { it.executeUpdate("backup to $dbSnapshotPath") }
        }

        log.info { "Uploading backup database file..." }
        executor.uploadFile(dbSnapshotPath, DB_FILENAME, backup = true)
        log.info { "Database upload complete!" }
    }

    private fun generateReport() {
        log.info { "Generating report..." }
        val workingReportPath = workflowTmpDir.resolve(REPORT_FILENAME)
        val uploadReportFile = "$RUN_DIR/${workflowRun.startTime}/$REPORT_FILENAME"

        Files.createDirectories(workingReportPath.parent)
        val writer = Files.newBufferedWriter(workingReportPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        createReport(db, workflowRun, writer)
        writer.flush()
        log.info { "Report generation complete (see: $workingReportPath)! Uploading..." }

        executor.uploadFile(workingReportPath, uploadReportFile)
        log.info { "Report upload complete!" }
    }
}
