package krews.config

data class TaskConfig (
    val params: Map<String, Any>,
    val env: Map<String, String>?,
    val google: GoogleTaskConfig?
)

data class WorkflowConfig (
    val params: Map<String, Any>,
    val local: LocalWorkflowConfig?,
    val google: GoogleWorkflowConfig?,
    val tasks: Map<String, TaskConfig>
)