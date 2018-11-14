package krews.db

import org.jetbrains.exposed.dao.*


object TaskRuns : IntIdTable("task_run") {
    val workflowRunId = reference("workflow_run", WorkflowRuns)
    val taskName = text("task_name")
    val startTime = long("start_time")
    val completedSuccessfully = bool("completed_successfully")
    val completedTime = long("completed_time").nullable()
    val inputJson = text("input_json")
    val command = text("command").nullable()
    val image = text("image")
    val outputJson = text("output_json").nullable()
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