package krews.db

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable


object WorkflowRuns : IntIdTable("workflow_run") {
    val workflowName = text("workflow_name")
    val startTime = datetime("start_time")
    val completedSuccessfully = bool("completed_successfully")
    val completedTime = datetime("completed_time").nullable()
}

class WorkflowRun(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<WorkflowRun>(WorkflowRuns)

    var workflowName by WorkflowRuns.workflowName
    var startTime by WorkflowRuns.startTime
    var completedSuccessfully by WorkflowRuns.completedSuccessfully
    var completedTime by WorkflowRuns.completedTime
}