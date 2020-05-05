package krews.db

import org.jetbrains.exposed.dao.*


object TaskRuns : IntIdTable("task_run") {
    val taskName = text("task_name")
    val startTime = long("start_time")
    // "completed", "partially completed", "failed"
    val completionStatus = varchar("completion_status", 20)
    val completedTime = long("completed_time").nullable()
    val executionsJson = text("executions_json")
}

class TaskRun(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<TaskRun>(TaskRuns)

    var taskName by TaskRuns.taskName
    var startTime by TaskRuns.startTime
    var completionStatus by TaskRuns.completionStatus
    var completedTime by TaskRuns.completedTime
    var executionsJson by TaskRuns.executionsJson
}

// TaskRuns.executionsJson is just a json serialized list of these objects.
data class TaskRunExecution(
    val image: String,
    val command: String?
)