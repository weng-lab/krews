package krews.db

import org.jetbrains.exposed.dao.*


object WorkflowRuns : IntIdTable("workflow_run") {
    val workflowName = text("workflow_name")
    val startTime = datetime("start_time")
    val completedSuccessfully = bool("completed_successfully")
    val completedTime = datetime("completed_time").nullable()
}

object TaskRuns : IntIdTable("task_run") {
    val workflowRunId = reference("workflow_run", WorkflowRuns)
    val taskName = text("task_name")
    val startTime = datetime("start_time")
    val completedSuccessfully = bool("completed_successfully")
    val completedTime = datetime("completed_time").nullable()
    val inputHash = integer("input_hash")
    val commandHash = integer("command_hash").nullable()
    val image = text("image")
    val outputJson = text("output_json")
    init {
        uniqueIndex("task_run_by_inputs", taskName, inputHash, commandHash, image)
    }
}

class WorkflowRun(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<WorkflowRun>(WorkflowRuns)

    var workflowName by WorkflowRuns.workflowName
    var startTime by WorkflowRuns.startTime
    var completedSuccessfully by WorkflowRuns.completedSuccessfully
    var completedTime by WorkflowRuns.completedTime
}

class TaskRun(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<TaskRun>(TaskRuns)

    var workflowRun by WorkflowRun referencedOn TaskRuns.workflowRunId
    var taskName by TaskRuns.taskName
    var startTime by TaskRuns.startTime
    var completedSuccessfully by TaskRuns.completedSuccessfully
    var completedTime by TaskRuns.completedTime
    var inputHash by TaskRuns.inputHash
    var commandHash by TaskRuns.commandHash
    var image by TaskRuns.image
    var outputJson by TaskRuns.outputJson
}