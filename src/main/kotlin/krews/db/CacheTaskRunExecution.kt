package krews.db

import org.jetbrains.exposed.dao.*

object CachedTaskRunExecutions : IntIdTable("cached_task_run_execution") {
    val latestUseWorkflowRunId = integer("latest_use_workflow_run_id")
    val taskName = text("task_name")
    val inputJson = text("input_json")
    val paramsJson = text("params_json").nullable()
    val command = text("command").nullable()
    val image = text("image")
    val outputFiles = text("output_files")
    init {
        uniqueIndex("cached_executions_by_inputs", taskName, inputJson, paramsJson, command, image)
    }
}

class CachedTaskRunExecution(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<CachedTaskRunExecution>(CachedTaskRunExecutions)

    var latestUseWorkflowRunId by CachedTaskRunExecutions.latestUseWorkflowRunId
    var taskName by CachedTaskRunExecutions.taskName
    var inputJson by CachedTaskRunExecutions.inputJson
    var paramsJson by CachedTaskRunExecutions.paramsJson
    var command by CachedTaskRunExecutions.command
    var image by CachedTaskRunExecutions.image
    var outputFiles by CachedTaskRunExecutions.outputFiles
}