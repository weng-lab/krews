package krews.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import krews.config.LimitedParallelism
import krews.config.UnlimitedParallelism
import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.*
import krews.file.InputFile
import krews.file.getInputFilesForObject
import krews.file.getOutputFilesForObject
import mu.KotlinLogging
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import reactor.core.Scannable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executors
import java.util.stream.Collectors


private val log = KotlinLogging.logger {}
private val mapper = jacksonObjectMapper()

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val executor: LocallyDirectedExecutor,
    private val runTimestampOverride: Long? = null
) {

    private val db = migrateAndConnectDb(executor.prepareDatabaseFile())
    private lateinit var workflowRun: WorkflowRun

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

        // Create an executor service for
        val workflowParallelism = workflowConfig.parallelism
        val executorService = when(workflowParallelism) {
            null, is UnlimitedParallelism -> Executors.newCachedThreadPool()
            is LimitedParallelism -> Executors.newFixedThreadPool(workflowParallelism.limit)
        }

        // Set execute function for each task.
        for (task in workflow.tasks.values) {
            task.executorService = executorService
            task.taskConfig = workflowConfig.tasks[task.name]
            task.executeFn = { command: String, inputItem: Any, outputItem: Any? ->
                runTask(task, command, inputItem, outputItem)
            }
            task.connect()
        }

        // Set execute function for each file import.
        for (fileImport in workflow.fileImports.values) {
            val executeFn: FileImportExecuteFn<Any> =
                { inputItem -> runFileImport(inputItem, fileImport.dockerDataDir) }
            fileImport.connect(executeFn)
        }

        // Get "leafOutputs", meaning this workflow's task.output fluxes that don't have other task.outputs as parents
        val allTaskOutputFluxes = workflow.tasks.values.map { it.output }
        val leafOutputs = allTaskOutputFluxes.toMutableSet()
        for (output in allTaskOutputFluxes) {
            val taskParents = Scannable.from(output).parents().collect(Collectors.toSet())
                .filter { it is Flux<*> }
                .map { it as Flux<*> }
            leafOutputs.removeAll(taskParents)
        }

        try {
            // Trigger workflow by subscribing to leaf task outputs...
            val leavesFlux = Flux.merge(leafOutputs)
            leavesFlux.subscribeOn(Schedulers.elastic())

            // and block until it's done
            leavesFlux.blockLast()
        } finally {
            executor.pushDatabaseFile()
        }
    }

    private fun runTask(task: Task<*, *>, command: String?, inputItem: Any, outputItem: Any?) {
        val taskConfig = workflowConfig.tasks[task.name]!!
        val taskName = task.name

        val inputJson = mapper.writeValueAsString(inputItem)
        log.info {
            "Running task \"${task.name}\" for dockerImage \"${task.dockerImage}\" input \"$inputJson\" " +
                    "output \"$outputItem\" command:\n$command"
        }

        val now = DateTime.now()
        val taskRun: TaskRun = transaction(db) {
            TaskRun.new {
                this.workflowRun = this@WorkflowRunner.workflowRun
                this.startTime = now.millis
                this.taskName = taskName
                this.inputJson = inputJson
                this.command = command
                this.image = task.dockerImage
            }
        }
        log.info { "Task run created with id ${taskRun.id.value}. Checking cache..." }
        val cachedOutputTasks: List<TaskRun> = transaction(db) {
            TaskRun.find {
                TaskRuns.taskName eq task.name and
                        (TaskRuns.image eq task.dockerImage) and
                        (TaskRuns.inputJson eq inputJson) and
                        (TaskRuns.command eq command) and
                        (TaskRuns.completedSuccessfully eq true)
            }.toList()
        }
        val latestCachedOutputTask: TaskRun? = cachedOutputTasks.maxBy { it.startTime }

        val inputFilesBySource = transaction(db) { inputFilesBySource(inputItem) }
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
            val outputFilesIn = getOutputFilesForObject(inputItem)
            val outputFilesOut = getOutputFilesForObject(outputItem)
            executor.executeTask(
                getWorkflowRunDir(workflowRun),
                taskRun.id.value,
                taskConfig,
                task.dockerImage,
                task.dockerDataDir,
                command,
                outputFilesIn,
                outputFilesOut,
                inputFilesBySource.cached,
                inputFilesBySource.download
            )
        } else {
            transaction(db) {
                log.info { "Valid cached outputs found. Skipping execution." }
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

        transaction(db) {
            log.info { "Task completed successfully. Saving status..." }

            // Create new input file records in db for files copied from remote sources during execution
            for (remoteInputFile in inputFilesBySource.download) {
                InputFileRecord.new {
                    this.path = remoteInputFile.path
                    this.lastModifiedTime = remoteInputFile.lastModified
                    this.workflowRun = this@WorkflowRunner.workflowRun
                }
            }

            // Add last modified timestamps to output files in task output
            // If we copied cached output files over, we should set it to the lastModified date of the file copied over.
            // This will downstream tasks with output files in their inputs to still use the cache properly even if
            // executor.outputFileLastModified returns something new.
            val outputFilesOut = getOutputFilesForObject(outputItem)
            for (outputFile in outputFilesOut) {
                outputFile.lastModified = cachedOutputLastModified[outputFile.path] ?:
                        executor.outputFileLastModified(getWorkflowOutputsDir(workflowRun), outputFile)
            }

            taskRun.outputJson = mapper.writeValueAsString(outputItem)
            taskRun.completedSuccessfully = true
            taskRun.completedTime = DateTime.now().millis
        }
    }

    private fun runFileImport(inputItem: Any, dockerDataDir: String) {
        log.info { "Running file import for inputItem $inputItem" }
        val (_, remoteInputFiles) = transaction(db) { inputFilesBySource(inputItem) }
        val workflowInputsDir = getWorkflowInputsDir(workflowRun)
        executor.downloadRemoteInputFiles(remoteInputFiles, dockerDataDir, workflowInputsDir)
    }
}

private fun getWorkflowRunDir(workflowRun: WorkflowRun) = "$RUN_DIR/${workflowRun.startTime}"
private fun getWorkflowOutputsDir(workflowRun: WorkflowRun) = "${getWorkflowRunDir(workflowRun)}/$OUTPUTS_DIR"
private fun getWorkflowInputsDir(workflowRun: WorkflowRun) = "${getWorkflowRunDir(workflowRun)}/$INPUTS_DIR"

/**
 * Gets a set of InputFiles out of the given inputItem, and splits them up into two categories: those that need to be
 * downloaded from cached (executor) storage and original sources.
 *
 * @param inputItem The item being processed, which may contain one or many input files.
 * @return Object containing "cached" input files (as records) that we will download from the record's run directory
 * and "download" input files that we need to download from the original input source
 */
private fun inputFilesBySource(inputItem: Any): InputFilesBySource {
    val inputFiles = getInputFilesForObject(inputItem)
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