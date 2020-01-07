package krews.misc

import com.fasterxml.jackson.module.kotlin.*
import krews.db.*
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.Writer

val statusMapper by lazy { jacksonObjectMapper() }

internal fun createStatusJson(db: Database, workflowRun: WorkflowRun, out: Writer) {
    transaction(db) {
        val taskRuns = TaskRun.all()
        createStatusJson(workflowRun, taskRuns, out)
    }
}

data class WorkflowRunStatus(
    val workflowName: String,
    val startTime: Long,
    val completedSuccessfully: Boolean,
    val completedTime: Long?
)

data class TaskRunStatus(
    val taskName: String,
    var startTime: Long,
    var completionStatus: TaskCompletionStatus,
    var completedTime: Long?,
    val executions: List<TaskRunExecution>
)

enum class TaskCompletionStatus {
    IN_PROGRESS, SUCCEEDED, PARTIALLY_SUCCEEDED, FAILED
}

data class Report(
    val workflowRunStatus: WorkflowRunStatus,
    val taskRunStatuses: List<TaskRunStatus>
)

private fun createStatusJson(workflowRun: WorkflowRun, taskRuns: Iterable<TaskRun>, out: Writer) {
    val workflowRunStatus = with(workflowRun) { WorkflowRunStatus(workflowName, startTime, completedSuccessfully, completedTime) }
    val taskRunStatuses = taskRuns.map { tr ->
        val taskRunExecutions = mapper.readValue<List<TaskRunExecution>>(tr.executionsJson)
        val taskCompletionStatus = when {
            tr.completedTime == null -> TaskCompletionStatus.IN_PROGRESS
            tr.completionStatus == "completed" -> TaskCompletionStatus.SUCCEEDED
            tr.completionStatus == "partially completed" -> TaskCompletionStatus.PARTIALLY_SUCCEEDED
            else -> TaskCompletionStatus.FAILED
        }
        TaskRunStatus(tr.taskName, tr.startTime, taskCompletionStatus, tr.completedTime, taskRunExecutions)
    }
    statusMapper.writeValue(out, Report(workflowRunStatus, taskRunStatuses))
}
