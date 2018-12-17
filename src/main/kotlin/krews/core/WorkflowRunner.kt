package krews.core

import krews.config.LimitedParallelism
import krews.config.UnlimitedParallelism
import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.*
import krews.file.getOutputFilesForObject
import krews.misc.createReport
import mu.KotlinLogging
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import reactor.core.Scannable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors


private val log = KotlinLogging.logger {}

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val executor: LocallyDirectedExecutor,
    private val runTimestampOverride: Long? = null
) {
    private val db: Database
    private lateinit var workflowRun: WorkflowRun

    init {
        executor.downloadFile(DB_FILENAME)
        db = migrateAndConnectDb(Paths.get(workflowConfig.localFilesBaseDir, DB_FILENAME))
    }

    fun run() {
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

        // Create an executor service for periodically generating reports
        val reportThreadFactory = BasicThreadFactory.Builder().namingPattern("report-gen-%d").build()
        val reportPool = Executors.newSingleThreadScheduledExecutor(reportThreadFactory)
        reportPool.scheduleWithFixedDelay({ generateReport() },
            workflowConfig.reportGenerationDelay, workflowConfig.reportGenerationDelay, TimeUnit.SECONDS)

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

            // Remove old workflowRuns / taskRuns from db
            WorkflowRuns.deleteWhere { WorkflowRuns.id neq workflowRun.id }
        }

        transaction(db) {
            // if config.cleanOldOutputs delete output files not created by this run
            if (workflowConfig.cleanOldFiles) {
                log.info { "Cleaning input and output files not used by this run..." }

                CachedInputFiles.deleteWhere { CachedInputFiles.latestUseWorkflowRunId neq workflowRun.id.value }
                val cachedInputFiles = CachedInputFile.all().flatMap { getOutputFilesForObject(it) }.map { it.path }.toSet()
                val inputPathFiles = executor.listFiles(INPUTS_DIR).map { it.substringAfter(INPUTS_DIR).replace("\\", "/") }
                for (inputPathFile in inputPathFiles) {
                    if (!cachedInputFiles.contains(inputPathFile)) {
                        log.info { "Deleting unused input file $inputPathFile" }
                        executor.deleteFile(inputPathFile)
                    }
                }
                CachedOutputs.deleteWhere { CachedOutputs.latestUseWorkflowRunId neq workflowRun.id.value }
                val cachedOutputFiles = CachedOutput.all().flatMap { getOutputFilesForObject(it) }.map { it.path }.toSet()
                val outputPathFiles = executor.listFiles(OUTPUTS_DIR).map { it.substringAfter(OUTPUTS_DIR).replace("\\", "/") }
                for (outputPathFile in outputPathFiles) {
                    if (!cachedOutputFiles.contains(outputPathFile)) {
                        log.info { "Deleting unused output file $outputPathFile" }
                        executor.deleteFile(outputPathFile)
                    }
                }
            }
        }

        // Stop the periodic report generation executor service and generate one final report.
        reportPool.shutdown()
        reportPool.awaitTermination(3000, TimeUnit.SECONDS)
        generateReport()

        executor.uploadFile(DB_FILENAME)
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
            val taskConfig = workflowConfig.tasks[task.name]
            val executeFn = { taskRunContext: TaskRunContext<*, *> ->
                taskRunner.run(task, taskRunContext)
            }
            task.connect(taskConfig, executeFn, workerPool)
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

        val successful = AtomicBoolean(true)

        // Trigger workflow by subscribing to leaf task outputs...
        val leavesFlux = Flux.merge(leafOutputs)
            .onErrorContinue { t: Throwable, _ ->
                successful.set(false)
                log.error(t) { }
            }
            .subscribeOn(Schedulers.elastic())

        // and block until it's done
        leavesFlux.blockLast()

        return successful.get()
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
