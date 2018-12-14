package krews.core

import krews.config.LimitedParallelism
import krews.config.UnlimitedParallelism
import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.*
import krews.file.InputFile
import krews.file.getInputFilesForObject
import krews.file.getOutputFilesForObject
import krews.misc.createReport
import krews.misc.mapper
import mu.KotlinLogging
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.and
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
        val reportExecutorService = Executors.newSingleThreadScheduledExecutor(reportThreadFactory)
        reportExecutorService.scheduleWithFixedDelay({ generateReport() },
            workflowConfig.reportGenerationDelay, workflowConfig.reportGenerationDelay, TimeUnit.SECONDS)

        // Create an executor service for executing tasks.
        // The system's combined task parallelism will be determined by this executor's thread limit
        val workerThreadFactory = BasicThreadFactory.Builder().namingPattern("worker-%d").build()
        val workflowParallelism = workflowConfig.parallelism
        val workerExecutorService = when (workflowParallelism) {
            is UnlimitedParallelism -> Executors.newCachedThreadPool(workerThreadFactory)
            is LimitedParallelism -> Executors.newFixedThreadPool(workflowParallelism.limit, workerThreadFactory)
        }

        // Set execute function for each task.
        for (task in workflow.tasks.values) {
            val taskConfig = workflowConfig.tasks[task.name]
            val executeFn = { taskRunContext: TaskRunContext<*, *> ->
                runTask(task, taskRunContext)
            }
            task.connect(taskConfig, executeFn, workerExecutorService)
        }

        // Set execute function for each file import.
        for (fileImport in workflow.fileImports.values) {
            val executeFn = { inputEl: Any -> runFileImport(inputEl, fileImport.dockerDataDir) }
            fileImport.connect(executeFn, workerExecutorService)
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
        try {
            // Trigger workflow by subscribing to leaf task outputs...
            val leavesFlux = Flux.merge(leafOutputs)
            leavesFlux.subscribeOn(Schedulers.elastic())
            leavesFlux.subscribe({}, { e ->
                successful.set(false)
                log.error(e) { }
            })

            // and block until it's done
            leavesFlux.blockLast()

            transaction(db) {
                if (workflowConfig.cleanOldRuns) {
                    val oldWorkflowRuns = WorkflowRun.find { WorkflowRuns.id neq workflowRun.id }
                    for (oldWorkflowRun in oldWorkflowRuns) {
                        executor.deleteDirectory(getWorkflowRunDir(oldWorkflowRun))
                    }
                    WorkflowRuns.deleteWhere { WorkflowRuns.id neq workflowRun.id }
                }
            }
        } catch (e: Exception) {
            successful.set(false)
            throw e
        } finally {
            transaction(db) {
                workflowRun.completedSuccessfully = successful.get()
                workflowRun.completedTime = DateTime.now().millis
            }

            // Stop the periodic report generation executor service and generate one final report.
            reportExecutorService.shutdown()
            reportExecutorService.awaitTermination(3000, TimeUnit.SECONDS)
            generateReport()

            executor.uploadFile(DB_FILENAME)
        }
    }

    private fun <I : Any, O : Any> runTask(task: Task<I, O>, taskRunContext: TaskRunContext<*, *>) {
        val taskConfig = workflowConfig.tasks[task.name]!!
        val taskName = task.name

        val inputJson = mapper.writerFor(task.inputClass).writeValueAsString(taskRunContext.input)
        log.info {
            """
            |Running task "${task.name}" for dockerImage "${taskRunContext.dockerImage}"
            |Input: $inputJson
            |Command:
            |${taskRunContext.command}
            """.trimMargin()
        }

        val paramsJson =
            if (taskRunContext.taskParams != null) {
                mapper.writerFor(taskRunContext.taskParamsClass).writeValueAsString(taskRunContext.taskParams)
            } else { null }

        val now = DateTime.now()
        val taskRun: TaskRun = transaction(db) {
            TaskRun.new {
                this.workflowRun = this@WorkflowRunner.workflowRun
                this.startTime = now.millis
                this.taskName = taskName
                this.inputJson = inputJson
                this.paramsJson = paramsJson
                this.command = taskRunContext.command
                this.image = taskRunContext.dockerImage
            }
        }

        val inputFilesBySource = transaction(db) { inputFilesBySource(taskRunContext.input, taskRunContext.taskParams) }

        try {
            log.info { "Task run created with id ${taskRun.id.value}. Checking cache..." }
            val cachedOutputTasks: List<TaskRun> = transaction(db) {
                TaskRun.find {
                    TaskRuns.taskName eq task.name and
                            (TaskRuns.image eq taskRunContext.dockerImage) and
                            (TaskRuns.inputJson eq inputJson) and
                            (TaskRuns.paramsJson eq paramsJson) and
                            (TaskRuns.command eq taskRunContext.command) and
                            (TaskRuns.completedSuccessfully eq true)
                }.toList()
            }
            val latestCachedOutputTask: TaskRun? = cachedOutputTasks.maxBy { it.startTime }

            val toInputDir = transaction(db) { getWorkflowInputsDir(workflowRun) }
            val prevRunInputFiles = inputFilesBySource.cached.filter { it.workflowInputsDir != toInputDir }
            if (prevRunInputFiles.isNotEmpty()) {
                log.info { "Copying input files $prevRunInputFiles from previous run input directories to $toInputDir" }
                for (inputFile in prevRunInputFiles) {
                    executor.copyCachedFiles(inputFile.workflowInputsDir, toInputDir, setOf(inputFile.path))

                    // Create new input file record in db for file copied into new workflow inputs dir
                    transaction(db) {
                        InputFileRecord.new {
                            this.path = inputFile.path
                            this.lastModifiedTime = inputFile.lastModified
                            this.workflowRun = this@WorkflowRunner.workflowRun
                        }
                    }
                }
            }

            val cachedOutputLastModified = mutableMapOf<String, Long>()
            if (latestCachedOutputTask == null) {
                log.info { "Valid cached outputs not found. Executing..." }
                val outputFilesIn = getOutputFilesForObject(taskRunContext.input)
                val outputFilesOut = getOutputFilesForObject(taskRunContext.output)
                executor.executeTask(
                    getWorkflowRunDir(workflowRun),
                    taskRun.id.value,
                    taskConfig,
                    taskRunContext,
                    outputFilesIn,
                    outputFilesOut,
                    inputFilesBySource.cached,
                    inputFilesBySource.download
                )
            } else {
                transaction(db) {
                    log.info { "Valid cached outputs found. Skipping execution." }
                    taskRun.cacheUsed = true
                    val cachedOutputWorkflowRunId = latestCachedOutputTask.workflowRun.id.value
                    val workflowRunId = workflowRun.id.value
                    if (cachedOutputWorkflowRunId == workflowRunId) {
                        log.info { "Cached values come from this workflow run. Skipping output file copy." }
                    } else {
                        val cachedOutput = mapper.readValue(latestCachedOutputTask.outputJson, task.outputClass)
                        val fromOutputDir = getWorkflowOutputsDir(latestCachedOutputTask.workflowRun)
                        val toOutputDir = getWorkflowOutputsDir(workflowRun)
                        val outputFiles = getOutputFilesForObject(cachedOutput)
                        val outputFilePaths = outputFiles.map { it.path }.toSet()
                        log.info { "Copying cached output files $outputFiles from $fromOutputDir to $toOutputDir" }
                        executor.copyCachedFiles(fromOutputDir, toOutputDir, outputFilePaths)
                        for (outputFile in outputFiles) {
                            cachedOutputLastModified[outputFile.path] = outputFile.lastModified
                        }
                    }
                }
            }

            // Add last modified timestamps to output files in task output
            // If we copied cached output files over, we should set it to the lastModified date of the file copied over.
            // This will downstream tasks with output files in their inputs to still use the cache properly even if
            // executor.outputFileLastModified returns something new.
            val outputFilesOut = getOutputFilesForObject(taskRunContext.output)
            for (outputFile in outputFilesOut) {
                outputFile.lastModified = cachedOutputLastModified[outputFile.path] ?:
                        executor.outputFileLastModified(getWorkflowOutputsDir(workflowRun), outputFile)
            }
            val outputJson = mapper.writerFor(task.outputClass).writeValueAsString(taskRunContext.output)

            transaction(db) {
                log.info {
                    """
                    |Task completed successfully!
                    |Output: $outputJson
                    |Saving status...
                    """.trimMargin()
                }

                // Create new input file records in db for files copied from remote sources during execution
                for (remoteInputFile in inputFilesBySource.download) {
                    InputFileRecord.new {
                        this.path = remoteInputFile.path
                        this.lastModifiedTime = remoteInputFile.lastModified
                        this.workflowRun = this@WorkflowRunner.workflowRun
                    }
                }

                taskRun.outputJson = outputJson
                taskRun.completedSuccessfully = true
                taskRun.completedTime = DateTime.now().millis
                log.info { "Task status save complete." }
            }
        } catch (e: Exception) {
            transaction(db) { taskRun.completedTime = DateTime.now().millis }
            throw e
        }

    }

    private fun runFileImport(input: Any, dockerDataDir: String) {
        log.info { "Running file import for inputEl $input" }
        val (_, remoteInputFiles) = transaction(db) { inputFilesBySource(input) }
        val workflowInputsDir = getWorkflowInputsDir(workflowRun)
        executor.downloadRemoteInputFiles(remoteInputFiles, dockerDataDir, workflowInputsDir)
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

private fun getWorkflowRunDir(workflowRun: WorkflowRun) = "$RUN_DIR/${workflowRun.startTime}"
private fun getWorkflowOutputsDir(workflowRun: WorkflowRun) = "${getWorkflowRunDir(workflowRun)}/$OUTPUTS_DIR"
private fun getWorkflowInputsDir(workflowRun: WorkflowRun) = "${getWorkflowRunDir(workflowRun)}/$INPUTS_DIR"

/**
 * Gets a set of InputFiles out of the given inputEl, and splits them up into two categories: those that need to be
 * downloaded from cached (executor) storage and original sources.
 *
 * @param input The input element being processed, which may contain one or many input files.
 * @param params Input parameters (optional) also being processed. May also contain input files.
 * @return Object containing "cached" input files (as records) that we will download from the record's run directory
 * and "download" input files that we need to download from the original input source
 */
private fun inputFilesBySource(input: Any, params: Any? = null): InputFilesBySource {
    val inputFiles = getInputFilesForObject(input) + getInputFilesForObject(params)
    val cached = mutableSetOf<CachedInputFile>()
    val download = mutableSetOf<InputFile>()

    for (inputFile in inputFiles) {
        val inputFileRecord = InputFileRecord
            .find { InputFileRecords.path eq inputFile.path }
            .sortedBy { InputFileRecords.lastModifiedTime }
            .lastOrNull()

        // If the latest record exists and has the same lastModified time as remote, use the record's local copy
        if (inputFileRecord != null && inputFileRecord.lastModifiedTime == inputFile.lastModified) {
            val localInputFile = CachedInputFile(
                inputFileRecord.path, getWorkflowInputsDir(inputFileRecord.workflowRun),
                inputFileRecord.lastModifiedTime
            )
            cached.add(localInputFile)
        } else {
            download.add(inputFile)
        }
    }
    return InputFilesBySource(cached, download)
}

private data class InputFilesBySource(val cached: Set<CachedInputFile>, val download: Set<InputFile>)