package krews.core

import krews.config.*
import krews.db.*
import krews.executor.*
import krews.misc.createReport
import krews.misc.createStatusJson
import mu.KotlinLogging
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.jetbrains.exposed.sql.*
import org.joda.time.DateTime
import reactor.core.publisher.*
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max


private val log = KotlinLogging.logger {}

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val taskConfigs: Map<String, TaskConfig>,
    private val executor: LocallyDirectedExecutor,
    runTimestampOverride: Long? = null
) {
    private val runRepo: RunRepo
    private val runDb: Database
    private lateinit var workflowRun: WorkflowRun
    private val reportPool: ScheduledExecutorService
    private val workflowTime = runTimestampOverride ?: DateTime.now().millis
    private val workflowTmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "workflow-${workflow.name}-$workflowTime")
    private val hasShutdown = AtomicBoolean(false)

    init {
        val runDbFilePath = workflowTmpDir.resolve(RUN_DB_FILENAME)
        if (Files.exists(runDbFilePath)) {
            Files.delete(runDbFilePath)
        }
        runDb = setupRunDb(runDbFilePath)
        postConnectionSetup()

        runRepo = RunRepo(runDb)
        val reportThreadFactory = BasicThreadFactory.Builder().namingPattern("report-gen-%d").build()
        reportPool = Executors.newSingleThreadScheduledExecutor(reportThreadFactory)
    }

    fun run() {
        // Create the workflow run in the database
        log.info { "Creating workflow run for workflow ${workflow.name} with timestamp $workflowTime" }
        workflowRun = runRepo.createWorkflowRun(workflow.name, workflowTime)

        log.info { "Workflow run created successfully!" }

        // Create an executor service for periodically generating reports
        val reportGenerationDelay = max(workflowConfig.reportGenerationDelay, 10)
        reportPool.scheduleWithFixedDelay({
            try {
                generateStatusReports()
            } catch (e: Exception) {
                log.error(e) { "Exception caught generating status reports." }
            }
        }, reportGenerationDelay, reportGenerationDelay, TimeUnit.SECONDS)

        val workflowRunSuccessful = runWorkflow()

        runRepo.completeWorkflowRun(workflowRun, workflowRunSuccessful)
        onShutdown()
    }

    private fun runWorkflow(): Boolean {
        val taskRunner = TaskRunner(workflowRun, workflowConfig, taskConfigs, executor, runRepo)
        try {
            // Set execute function for each task.
            for (task in workflow.tasks.values) {
                task.connect(taskConfigs[task.name], taskRunner)
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
        // Stop the periodic report generation executor service and generate one final report.
        reportPool.awaitTermination(10, TimeUnit.SECONDS)

        generateStatusReports()

        Files.walk(workflowTmpDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
        log.info { "Shutdown complete!" }
    }

    /**
     * Create and upload human-readable html status report and machine-readable json status file.
     */
    private fun generateStatusReports() {
        if (!runRepo.taskUpdatedSinceLastReport.get()) {
            log.info { "No updates since last report generation. Skipping..." }
            return
        }
        runRepo.taskUpdatedSinceLastReport.set(false)

        log.info { "Generating html report..." }
        val workingHtmlPath = workflowTmpDir.resolve(REPORT_FILENAME)
        val uploadHtmlFile = "$RUN_DIR/${workflowRun.startTime}/$REPORT_FILENAME"

        Files.createDirectories(workingHtmlPath.parent)
        val htmlWriter = Files.newBufferedWriter(workingHtmlPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        createReport(runDb, workflowRun, htmlWriter)
        htmlWriter.flush()
        log.info { "Html Report generation complete (see: $workingHtmlPath)!" }

        log.info { "Generating json status file..." }
        val workingJsonPath = workflowTmpDir.resolve(STATUS_JSON_FILENAME)
        val uploadJsonFile = "$RUN_DIR/${workflowRun.startTime}/$STATUS_JSON_FILENAME"

        Files.createDirectories(workingJsonPath.parent)
        val jsonWriter = Files.newBufferedWriter(workingJsonPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        createStatusJson(runDb, workflowRun, jsonWriter)

        log.info { "Json status file generation complete (see: $workingJsonPath)!" }

        log.info { "Uploading status files..." }
        executor.uploadFile(workingHtmlPath, uploadHtmlFile)
        executor.uploadFile(workingJsonPath, uploadJsonFile)
        log.info { "Status files upload complete!" }
    }
}
