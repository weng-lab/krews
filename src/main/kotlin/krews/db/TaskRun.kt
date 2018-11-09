package krews.db

import org.jetbrains.exposed.dao.*


object TaskRuns : IntIdTable("task_run") {
    val workflowRunId = reference("workflow_run", WorkflowRuns)
    val taskName = text("task_name")
    val startTime = datetime("start_time")
    val completedSuccessfully = bool("completed_successfully")
    val completedTime = datetime("completed_time").nullable()
    val inputJson = text("input_hash")
    val command = text("command_hash").nullable()
    val image = text("image")
    val outputJson = text("output_json")
    init {
        uniqueIndex("task_run_by_inputs", taskName, inputJson, command, image)
    }
}

class TaskRun(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<TaskRun>(TaskRuns)

    var workflowRun by WorkflowRun referencedOn TaskRuns.workflowRunId
    var taskName by TaskRuns.taskName
    var startTime by TaskRuns.startTime
    var completedSuccessfully by TaskRuns.completedSuccessfully
    var completedTime by TaskRuns.completedTime
    var inputJson by TaskRuns.inputJson
    var command by TaskRuns.command
    var image by TaskRuns.image
    var outputJson by TaskRuns.outputJson
}