package krews.core

import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.*
import krews.misc.createReport
import mu.KotlinLogging
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.*
import org.joda.time.DateTime
import reactor.core.publisher.*
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.nio.file.attribute.PosixFilePermissions
import java.sql.Connection
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean


private val log = KotlinLogging.logger {}

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val executor: LocallyDirectedExecutor,
    runTimestampOverride: Long? = null
) {
    private val runRepo: RunRepo
    private val runDb: Database
    private val cacheDb: Database
    private lateinit var workflowRun: WorkflowRun
    private val reportPool: ScheduledExecutorService
    private val dbUploadPool: ScheduledExecutorService
    private val workflowTime = if (runTimestampOverride != null) runTimestampOverride else DateTime.now().millis
    private val workflowTmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "workflow-${workflow.name}-$workflowTime")
    private val hasShutdown = AtomicBoolean(false)

    init {
        val cacheDbFilePath = workflowTmpDir.resolve(CACHE_DB_FILENAME)
        executor.downloadFile(CACHE_DB_FILENAME, cacheDbFilePath)
        cacheDb = setupCacheDb(cacheDbFilePath)

        val runDbFilePath = workflowTmpDir.resolve(RUN_DB_FILENAME)
        if (Files.exists(runDbFilePath)) {
            Files.delete(runDbFilePath)
        }
        runDb = setupRunDb(runDbFilePath)
        postConnectionSetup()

        runRepo = RunRepo(runDb)

        val dbPermissions = PosixFilePermissions.fromString("rwxrwxrwx")
        Files.setPosixFilePermissions(cacheDbFilePath, dbPermissions)
        Files.setPosixFilePermissions(cacheDbFilePath.parent, dbPermissions)

        val dbUploadThreadFactory = BasicThreadFactory.Builder().namingPattern("db-upload-%d").build()
        dbUploadPool = Executors.newSingleThreadScheduledExecutor(dbUploadThreadFactory)

        val reportThreadFactory = BasicThreadFactory.Builder().namingPattern("report-gen-%d").build()
        reportPool = Executors.newSingleThreadScheduledExecutor(reportThreadFactory)
    }

    fun run() {
        // Create the workflow run in the database
        log.info { "Creating workflow run for workflow ${workflow.name} with timestamp $workflowTime" }
        workflowRun = runRepo.createWorkflowRun(workflow.name, workflowTime)

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

        runRepo.completeWorkflowRun(workflowRun, workflowRunSuccessful)
        onShutdown()
    }

    private fun runWorkflow(): Boolean {
        val taskRunner = TaskRunner(workflowRun, workflowConfig, executor, runRepo, cacheDb)
        try {
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

            val failedTasks = runRepo.failedTasksCount()
            return failedTasks == 0
        } catch(e: Exception) {
            log.error(e) { "Workflow unsuccessful." }
            return false
        } finally {
            taskRunner.stop()
        }
    }

    fun onShutdown() {
        if (!hasShutdown.compareAndSet(false, true)) return
        log.info { "Shutting down..." }
        reportPool.shutdown()
        dbUploadPool.shutdown()
        // Stop the periodic db upload and report generation executor service and generate one final report.
        reportPool.awaitTermination(10, TimeUnit.SECONDS)
        dbUploadPool.awaitTermination(10, TimeUnit.SECONDS)

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
        val dbSnapshotPath = workflowTmpDir.resolve(CACHE_DB_SNAPSHOT_FILENAME)
        log.info { "Backing up database..." }
        transaction(cacheDb) {
            TransactionManager.current().connection.createStatement().use { it.executeUpdate("backup to $dbSnapshotPath") }
        }

        log.info { "Uploading backup database file..." }
        executor.uploadFile(dbSnapshotPath, CACHE_DB_FILENAME, backup = true)
        log.info { "Database upload complete!" }
    }

    private fun generateReport() {
        if (!runRepo.taskUpdatedSinceLastReport.get()) {
            log.info { "No updates since last report generation. Skipping..." }
            return
        }
        runRepo.taskUpdatedSinceLastReport.set(false)

        log.info { "Generating report..." }
        val workingReportPath = workflowTmpDir.resolve(REPORT_FILENAME)
        val uploadReportFile = "$RUN_DIR/${workflowRun.startTime}/$REPORT_FILENAME"

        Files.createDirectories(workingReportPath.parent)
        val writer = Files.newBufferedWriter(workingReportPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        createReport(runDb, workflowRun, writer)
        writer.flush()
        log.info { "Report generation complete (see: $workingReportPath)! Uploading..." }

        executor.uploadFile(workingReportPath, uploadReportFile)
        log.info { "Report upload complete!" }
    }
}
