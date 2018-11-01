package krews

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.db.TaskRun
import krews.db.TaskRuns
import krews.db.WorkflowRun
import krews.db.migrateAndConnectDb
import krews.executor.LocallyDirectedExecutor
import krews.executor.getFilesForObject
import mu.KotlinLogging
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import reactor.core.Scannable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.stream.Collectors


private val log = KotlinLogging.logger {}

val outputMapper = jacksonObjectMapper()

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
        workflow.tasks.forEach { task ->
            task.executeFn = { command, inputItem, outputItem ->
                runTask(workflowRun, task.name, workflowConfig.tasks[task.name]!!, task.dockerImage, task.dockerDataDir,
                    command, inputItem, outputItem, task.outputClass)
            }
            task.connect()
        }

        // Get "leafOutputs", meaning this workflow's task.output fluxes that don't have other task.outputs as parents
        val allTaskOutputFluxes = workflow.tasks.map { it.output }
        val leafOutputs = allTaskOutputFluxes.toMutableSet()
        allTaskOutputFluxes.forEach { output ->
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

    private fun runTask(workflowRun: WorkflowRun, taskName: String, taskConfig: TaskConfig, dockerImage: String, dockerDataDir: String,
                        command: String?, inputItem: Any, outputItem: Any?, outputClass: Class<*>) {
        log.info { "Running task \"$taskName\" for dockerImage \"$dockerImage\" input \"$inputItem\" " +
                "output \"$outputItem\" command:\n$command" }

        val inputHash = inputItem.hashCode()
        val commandHash = command?.hashCode()

        log.info { "Checking cache..."}
        val cachedOutputTasks: List<TaskRun> = transaction(db) {
            TaskRun.find {
                TaskRuns.taskName eq taskName and
                        (TaskRuns.image eq dockerImage) and
                        (TaskRuns.inputHash eq inputHash) and
                        (TaskRuns.commandHash eq commandHash) and
                        (TaskRuns.completedSuccessfully eq true)
            }.toList()
        }
        val latestCachedOutputTask: TaskRun? = cachedOutputTasks.maxBy { it.startTime }

        val now = DateTime.now()
        val taskRun: TaskRun = transaction(db) {
            TaskRun.new {
                this.workflowRun = this@WorkflowRunner.workflowRun
                this.startTime = now
                this.taskName = taskName
                this.inputHash = inputHash
                this.commandHash = commandHash
                this.image = dockerImage
                this.outputJson = outputMapper.writeValueAsString(outputItem)
            }
        }
        log.info { "Task run created with id ${taskRun.id.value} and timestamp ${now.millis}" }

        transaction(db) {
            // If we have a cached output we can use for this task that's not from this run, copy the files over.
            // If it is from this run, the file should already exist, so it shouldn't need to be copied.
            if (latestCachedOutputTask != null && latestCachedOutputTask.workflowRun.id.value != workflowRun.id.value) {
                log.info { "Cached outputs found. Copying..." }
                val cachedOutput = outputMapper.readValue(latestCachedOutputTask.outputJson, outputClass)
                val outputFiles = getFilesForObject(cachedOutput)
                executor.copyCachedOutputs(getWorkflowRunDir(latestCachedOutputTask.workflowRun), getWorkflowRunDir(workflowRun), outputFiles)
            }

            // Only execute if we aren't using the cached value
            if (latestCachedOutputTask == null) {
                log.info { "Cached outputs not found. Executing..." }
                executor.executeTask(getWorkflowRunDir(workflowRun), taskRun.id.value, taskConfig, dockerImage,
                    dockerDataDir, command, inputItem, outputItem)
            }

            log.info { "Task completed successfully. Saving status..." }
            taskRun.completedSuccessfully = true
            taskRun.completedTime = DateTime.now()
        }
    }
}

private fun getWorkflowRunDir(workflowRun: WorkflowRun) = workflowRun.startTime.millis.toString()