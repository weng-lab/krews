package krews.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.*
import krews.file.InputFile
import mu.KotlinLogging
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import reactor.core.Scannable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.stream.Collectors


private val log = KotlinLogging.logger {}
private val mapper = jacksonObjectMapper()

class WorkflowRunner(private val workflow: Workflow,
                     private val workflowConfig: WorkflowConfig,
                     private val executor: LocallyDirectedExecutor,
                     private val runTimestampOverride: Long? = null) {

    private val db = migrateAndConnectDb(executor.prepareDatabaseFile())
    private lateinit var workflowRun: WorkflowRun

    fun run() {
        // Create the workflow run in the database
        val workflowTime = if (runTimestampOverride != null) DateTime(runTimestampOverride) else DateTime.now()
        log.info { "Creating workflow run for workflow ${workflow.name} with timestamp ${workflowTime.millis}" }
        transaction(db) {
            workflowRun = WorkflowRun.new {
                workflowName = workflow.name
                startTime = workflowTime
            }
        }
        log.info { "Workflow run created successfully!" }

        // Set execute function for each task.
        for (task in workflow.tasks) {
            task.executeFn = { command, inputItem, outputItem -> runTask(task, command, inputItem, outputItem) }
            task.connect()
        }

        // Set execute function for each file import.
        for (fileImport in workflow.fileImports) {
            fileImport.executeFn = { inputItem -> runFileImport(inputItem, fileImport.dockerDataDir) }
            fileImport.connect()
        }

        // Get "leafOutputs", meaning this workflow's task.output fluxes that don't have other task.outputs as parents
        val allTaskOutputFluxes = workflow.tasks.map { it.output }
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
            leavesFlux.subscribeOn(Schedulers.elastic()).subscribe()

            // and block until it's done
            leavesFlux.blockLast()
        } finally {
            executor.pushDatabaseFile()
        }
    }

    private fun runTask(task: Task<*,*>, command: String?, inputItem: Any, outputItem: Any?) {
        val taskConfig = workflowConfig.tasks[task.name]!!
        val taskName = task.name

        val inputJson = mapper.writeValueAsString(inputItem)
        log.info { "Running task \"${task.name}\" for dockerImage \"${task.dockerImage}\" input \"$inputJson\" " +
                "output \"$outputItem\" command:\n$command" }

        val now = DateTime.now()
        val taskRun: TaskRun = transaction(db) {
            TaskRun.new {
                this.workflowRun = this@WorkflowRunner.workflowRun
                this.startTime = now
                this.taskName = taskName
                this.inputJson = inputJson
                this.command = command
                this.image = task.dockerImage
                this.outputJson = mapper.writeValueAsString(outputItem)
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
        val toInputDir = getWorkflowInputsDir(workflowRun)
        val prevRunInputFiles = inputFilesBySource.local.filter { it.workflowInputsDir == toInputDir }
        if (prevRunInputFiles.isNotEmpty()) {
            log.info { "Copying input files $prevRunInputFiles from previous run input directories to $toInputDir" }
            for (inputFile in prevRunInputFiles) {
                executor.copyCachedFiles(inputFile.workflowInputsDir, toInputDir, setOf(inputFile.path))

                // Create new input file record in db for file copied into new workflow inputs dir
                transaction(db) {
                    InputFileRecord.new {
                        this.path = inputFile.path
                        this.lastModifiedTime = DateTime(inputFile.lastModified)
                        this.workflowRun = this@WorkflowRunner.workflowRun
                    }
                }
            }
        }

        if (latestCachedOutputTask == null) {
            log.info { "Valid cached outputs not found. Executing..." }
            val outputFilesIn = getOutputFilesForObject(inputItem)
            val outputFilesOut = getOutputFilesForObject(outputItem)
            executor.executeTask(
                getWorkflowRunDir(workflowRun), taskRun.id.value, taskConfig, task.dockerImage,
                task.dockerDataDir, command, outputFilesIn, outputFilesOut, inputFilesBySource.local, inputFilesBySource.remote
            )
        } else {
            log.info { "Valid cached outputs found. Skipping execution." }
            if (latestCachedOutputTask.workflowRun.id.value == workflowRun.id.value) {
                log.info { "Cached values come from this workflow run. Skipping output file copy." }
            } else {
                val cachedOutput = mapper.readValue(latestCachedOutputTask.outputJson, task.outputClass)
                val fromOutputDir = getWorkflowOutputsDir(latestCachedOutputTask.workflowRun)
                val toOutputDir = getWorkflowOutputsDir(workflowRun)
                val outputFiles = getOutputFilesForObject(cachedOutput).map { it.path }.toSet()
                log.info { "Copying cached output files $outputFiles from $fromOutputDir to $toOutputDir" }
                executor.copyCachedFiles(fromOutputDir, toOutputDir, outputFiles)
            }
        }

        transaction(db) {
            log.info { "Task completed successfully. Saving status..." }

            // Create new input file records in db for files copied from remote sources during execution
            for (remoteInputFile in inputFilesBySource.remote) {
                InputFileRecord.new {
                    this.path = remoteInputFile.path
                    this.lastModifiedTime = remoteInputFile.lastModified
                    this.workflowRun = this@WorkflowRunner.workflowRun
                }
            }

            taskRun.completedSuccessfully = true
            taskRun.completedTime = DateTime.now()
        }
    }

    private fun runFileImport(inputItem: Any, dockerDataDir: String) {
        log.info { "Running file import for inputItem $inputItem" }
        val (_, remoteInputFiles) = transaction(db) { inputFilesBySource(inputItem) }
        executor.downloadRemoteInputFiles(remoteInputFiles, dockerDataDir)
    }
}

private fun getWorkflowRunDir(workflowRun: WorkflowRun) = "$RUN_DIR/${workflowRun.startTime.millis}"
private fun getWorkflowOutputsDir(workflowRun: WorkflowRun) = "${getWorkflowRunDir(workflowRun)}/$OUTPUTS_DIR"
private fun getWorkflowInputsDir(workflowRun: WorkflowRun) = "${getWorkflowRunDir(workflowRun)}/$INPUTS_DIR"

/**
 * Gets a set of InputFiles out of the given inputItem, and splits them up into two categories: those that need to be
 * downloaded from local (executor) storage and remote sources.
 *
 * @param inputItem The item being processed, which may contain one or many input files.
 * @return Object containing "local" input files (as records) that we will download from the record's run directory
 * and "remote" input files that we need to download from remote source
 */
private fun inputFilesBySource(inputItem: Any): InputFilesBySource {
    val inputFiles = getInputFilesForObject(inputItem)
    val local = mutableSetOf<LocalInputFile>()
    val remote = mutableSetOf<InputFile>()

    for (inputFile in inputFiles) {
        val inputFileRecord = InputFileRecord
            .find { InputFileRecords.path eq inputFile.path }
            .sortedBy { InputFileRecords.lastModifiedTime }
            .lastOrNull()

        // If the latest record exists and has the same lastModified time as remote, use the record's local copy
        if (inputFileRecord != null && inputFileRecord.lastModifiedTime == inputFile.lastModified) {
            val localInputFile = LocalInputFile(inputFileRecord.path, getWorkflowInputsDir(inputFileRecord.workflowRun),
                inputFileRecord.lastModifiedTime.millis)
            local.add(localInputFile)
        } else {
            remote.add(inputFile)
        }
    }
    return InputFilesBySource(local, remote)
}

private data class InputFilesBySource(val local: Set<LocalInputFile>, val remote: Set<InputFile>)