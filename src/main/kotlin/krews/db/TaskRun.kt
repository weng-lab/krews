package krews.db

import org.jetbrains.exposed.dao.*


object TaskRuns : IntIdTable("task_run") {
    val taskName = text("task_name")
    val startTime = long("start_time")
    val completedSuccessfully = bool("completed_successfully")
    val completedTime = long("completed_time").nullable()
    val executionsJson = text("executions_json")
}

class TaskRun(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<TaskRun>(TaskRuns)

    var taskName by TaskRuns.taskName
    var startTime by TaskRuns.startTime
    var completedSuccessfully by TaskRuns.completedSuccessfully
    var completedTime by TaskRuns.completedTime
    var executionsJson by TaskRuns.executionsJson
}

// TaskRuns.executionsJson is just a json serialized list of these objects.
data class TaskRunExecution(
    val image: String,
    val command: String?
)