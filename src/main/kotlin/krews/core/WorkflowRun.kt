package krews.core

data class WorkflowRun(
    val workflowName: String,
    val startTime: Long,
    var completedSuccessfully: Boolean = false,
    var completedTime: Long? = null,
    val taskRuns: MutableSet<TaskRun> = mutableSetOf()
)

data class TaskRun(
    val taskRunContext: TaskRunContext<*, *>,
    val cacheUsed: Boolean,
    var startTime: Long? = null,
    var completedSuccessfully: Boolean? = null,
    var completedTime: Long? = null
)