package krews.core

import krews.config.LimitedParallelism
import krews.config.UnlimitedParallelism
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
import reactor.core.Scannable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors


private val log = KotlinLogging.logger {}

const val DB_SNAPSHOT_FILENAME = "$DB_FILENAME.snapshot"

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val executor: LocallyDirectedExecutor,
    private val runTimestampOverride: Long? = null
) {
    private val db: Database
    private lateinit var workflowRun: WorkflowRun
    private val reportPool: ScheduledExecutorService
    private val dbUploadPool: ScheduledExecutorService?

    init {
        executor.downloadFile(DB_FILENAME)
        db = migrateAndConnectDb(Paths.get(workflowConfig.localFilesBaseDir, DB_FILENAME))

        if (executor.uploadsDb()) {
            val dbUploadThreadFactory = BasicThreadFactory.Builder().namingPattern("db-upload-%d").build()
            dbUploadPool = Executors.newSingleThreadScheduledExecutor(dbUploadThreadFactory)
        } else {
            dbUploadPool = null
        }

        val reportThreadFactory = BasicThreadFactory.Builder().namingPattern("report-gen-%d").build()
        reportPool = Executors.newSingleThreadScheduledExecutor(reportThreadFactory)
    }

    fun run() {
        transaction(db) {
            WorkflowRuns.deleteAll()
            TaskRuns.deleteAll()
        }

        // Create the workflow run in the database
        val workflowTime = if (runTimestampOverride != null) DateTime(runTimestampOverride) else DateTime.now()
        log.info { "Creating workflow run for workflow ${workflow.name} with timestamp ${workflowTime.millis}" }
        transaction(db) {
            workflowRun = WorkflowRun.new {
                workflowName = workflow.name
                startTime = workflowTime.millis
            }
        }
        log.info { "Workflow run created successfully!" }

        // Create an executor service for periodically uploading the db file
        if (executor.uploadsDb()) {
            val dbUploadDelay = Math.max(workflowConfig.dbUploadDelay, 30)
            dbUploadPool!!.scheduleWithFixedDelay({ uploadDb() },
                dbUploadDelay, dbUploadDelay, TimeUnit.SECONDS)
        }

        // Create an executor service for periodically generating reports
        val reportGenerationDelay = Math.max(workflowConfig.reportGenerationDelay, 10)
        reportPool.scheduleWithFixedDelay({ generateReport() },
            reportGenerationDelay, reportGenerationDelay, TimeUnit.SECONDS)

        var workflowRunSuccessful: Boolean
        try {
            workflowRunSuccessful = runWorkflow()
        } catch (e: Exception) {
            workflowRunSuccessful = false
            log.error(e) { }
        }

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
        // Create an executor service for executing tasks.
        // The system's combined task parallelism will be determined by this executor's thread limit
        val workerThreadFactory = BasicThreadFactory.Builder().namingPattern("worker-%d").build()
        val workflowParallelism = workflowConfig.parallelism
        val workerPool = when (workflowParallelism) {
            is UnlimitedParallelism -> Executors.newCachedThreadPool(workerThreadFactory)
            is LimitedParallelism -> Executors.newFixedThreadPool(workflowParallelism.limit, workerThreadFactory)
        }

        val taskRunner = TaskRunner(workflowRun, workflowConfig, executor, db)
        // Set execute function for each task.
        for (task in workflow.tasks.values) {
            connectTask(task, taskRunner, workerPool)
        }

        // Get "leafOutputs", meaning this workflow's task.output fluxes that don't have other task.outputs as parents
        val allTaskOutputFluxes = workflow.tasks.values.map { it.outputPub }
        val leafOutputs = allTaskOutputFluxes.toMutableSet()
        for (output in allTaskOutputFluxes) {
            val taskParents = Scannable.from(output).parents().collect(Collectors.toSet())
                .filter { it is Flux<*> }
                .map { it as Flux<*> }
            leafOutputs.removeAll(taskParents)
        }

        // Trigger workflow by subscribing to leaf task outputs...
        val leavesFlux = Flux.merge(leafOutputs)
            .subscribeOn(Schedulers.elastic())

        // and block until it's done
        leavesFlux.blockLast()

        val failedTasks = transaction(db) {
            TaskRuns.select {
                TaskRuns.workflowRunId eq workflowRun.id and TaskRuns.completedSuccessfully.eq(false)
            }.count()
        }
        return failedTasks == 0
    }

    private fun <I : Any, O : Any> connectTask(task: Task<I, O>, taskRunner: TaskRunner, workerPool: ExecutorService) {
        val taskConfig = workflowConfig.tasks[task.name]
        val executeFn: (TaskRunContext<I, O>) -> O = { taskRunContext ->
            taskRunner.run(task, taskRunContext)
        }

        task.connect(taskConfig, executeFn, workerPool)
    }

    fun onShutdown() {
        reportPool.shutdown()
        dbUploadPool?.shutdown()
        // Stop the periodic db upload and report generation executor service and generate one final report.
        reportPool.awaitTermination(1000, TimeUnit.SECONDS)
        dbUploadPool?.awaitTermination(1000, TimeUnit.SECONDS)

        if (executor.uploadsDb()) uploadDb()
        generateReport()
    }

    /*
     * Backs up the sqlite database file and uploads the backup.
     * This is needed because it's not safe to copy a file that's currently open for writes.
     */
    private fun uploadDb() {
        // Create backup db file
        val dbSnapshotPath = Paths.get(workflowConfig.localFilesBaseDir, DB_SNAPSHOT_FILENAME)
        transaction(db) {
            TransactionManager.current().connection.createStatement().use { it.executeUpdate("backup to $dbSnapshotPath") }
        }
        executor.uploadFile(DB_SNAPSHOT_FILENAME, DB_FILENAME, backup = true)
    }

    private fun generateReport() {
        val reportFile = "$RUN_DIR/${workflowRun.startTime}/$REPORT_FILENAME"
        val reportPath = Paths.get(workflowConfig.localFilesBaseDir, reportFile)
        Files.createDirectories(reportPath.parent)
        val writer = Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        createReport(db, workflowRun, writer)
        writer.flush()
        executor.uploadFile(reportFile)
    }
}
