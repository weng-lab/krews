package krews

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.EnvironmentExecutor
import krews.executor.LocalExecutor
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import reactor.core.Scannable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.stream.Collectors

val outputMapper = jacksonObjectMapper()

class WorkflowRunner(private val workflow: Workflow, private val workflowConfig: WorkflowConfig) {

    private val executor: EnvironmentExecutor
    private val db: Database
    private lateinit var workflowRun: WorkflowRun

    init {
        //TODO: Add logic to pick environment here when we have more than just local
        executor = LocalExecutor(workflowConfig)
        db = migrateAndConnectDb(executor.prepareDatabaseFile())
    }

    fun run() {

        transaction(db) {
            workflowRun = WorkflowRun.new {
                workflowName = workflow.name
                startTime = DateTime.now()
            }
        }

        // Set execute function for each task.
        workflow.tasks.forEach { task ->
            task.executeFn = { script, inputItem, outputItem ->
                runTask(workflowRun, task.name, workflowConfig.tasks[task.name]!!, task.image, script, inputItem, outputItem)
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

        // Trigger workflow by subscribing to leaf task outputs
        val leavesFlux = Flux.merge(leafOutputs)
        leavesFlux.subscribeOn(Schedulers.elastic()).subscribe(::print, ::print)
        leavesFlux.blockLast()
    }

    private fun runTask(workflowRun: WorkflowRun, taskName: String, taskConfig: TaskConfig,
                        image: String, script: String?, inputItem: Any, outputItem: Any?) {
        val inputHash = inputItem.hashCode()
        val scriptHash = script?.hashCode()

        val cachedOutputTasks: List<TaskRun> = transaction(db) {
            TaskRun.find {
                TaskRuns.taskName eq taskName and
                        (TaskRuns.image eq image) and
                        (TaskRuns.inputHash eq inputHash) and
                        (TaskRuns.scriptHash eq scriptHash) and
                        (TaskRuns.completedSuccessfully eq true)
            }.toList()
        }
        val latestCachedOutputTask: TaskRun? = cachedOutputTasks.maxBy { it.startTime }

        var taskRun: TaskRun? = null
        transaction(db) {
            taskRun = TaskRun.new {
                this.workflowRun = this@WorkflowRunner.workflowRun
                this.startTime = DateTime.now()
                this.taskName = taskName
                this.inputHash = inputHash
                this.scriptHash = scriptHash
                this.image = image
                this.outputJson = outputMapper.writeValueAsString(outputItem)
            }
        }

        // If we have a cached output we can use for this task that's not from this run, copy the files over.
        // If it is from this run, the file should already exist, so it shouldn't need to be copied.
        if (latestCachedOutputTask != null && latestCachedOutputTask.workflowRun.id.value != workflowRun.id.value) {
            executor.copyCachedOutputs(workflowRun, latestCachedOutputTask)
        }

        // Only execute if we aren't using the cached value
        if (latestCachedOutputTask == null) {
            transaction(db) {
                executor.executeTask(workflowRun, taskConfig, image, script, inputItem, outputItem)
            }
        }

        transaction(db) {
            taskRun!!.completedSuccessfully = true
            taskRun!!.completedTime = DateTime.now()
        }
    }
}