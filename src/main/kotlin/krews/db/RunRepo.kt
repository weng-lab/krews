package krews.db

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import java.util.concurrent.atomic.AtomicBoolean

class RunRepo(private val runDb: Database) {

    val taskUpdatedSinceLastReport = AtomicBoolean(true)

    fun createWorkflowRun(workflowName: String, startTime: Long): WorkflowRun = transaction(runDb) {
        WorkflowRun.new {
            this.workflowName = workflowName
            this.startTime = startTime
        }
    }

    fun completeWorkflowRun(workflowRun: WorkflowRun, successful: Boolean) = transaction(runDb) {
        workflowRun.completedSuccessfully = successful
        workflowRun.completedTime = DateTime.now().millis
    }

    fun workflowStartTime(workflowRun: WorkflowRun) = transaction(runDb) {
        workflowRun.startTime
    }

    fun failedTasksCount() = transaction(runDb) {
        TaskRuns.select {
            TaskRuns.completionStatus.neq("completed")
        }.count()
    }

    fun createTaskRun(taskName: String, executionsJson: String) = transaction(runDb) {
        taskUpdatedSinceLastReport.set(true)
        TaskRun.new {
            this.startTime = DateTime.now().millis
            this.taskName = taskName
            this.executionsJson = executionsJson
        }
    }

    fun completeTaskRun(taskRun: TaskRun, completed: String) = transaction(runDb) {
        taskUpdatedSinceLastReport.set(true)
        taskRun.completionStatus = completed
        taskRun.completedTime = DateTime.now().millis
    }

}