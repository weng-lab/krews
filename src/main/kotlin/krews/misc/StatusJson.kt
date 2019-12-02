package krews.misc

import krews.db.*
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction

internal fun createStatusJson(db: Database, workflowRun: WorkflowRun, out: Appendable) {
    transaction(db) {
        val taskRuns = TaskRun.all()
        createStatusJson(workflowRun, taskRuns, out)
    }
}

data class WorkflowRunStatus(
    val workflowName: String,
    val startTime: Long,
    val completedSuccessfully: Boolean,
    val completedTime: Long
)

data class StatusTaskRun(
    val taskName: String,
    var startTime: Long,
    var completionStatus: String,
    var completedTime: Long,
    val executions: List<TaskRunExecution>
)

private fun createStatusJson(workflowRun: WorkflowRun, taskRuns: Iterable<TaskRun>, out: Appendable) {

}
