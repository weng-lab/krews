package krews.config

data class TaskConfig (
    val env: Map<String, String>?
)

data class WorkflowConfig (
    val params: Map<String, Any>,
    val localExec: LocalExecConfig?,
    val tasks: Map<String, TaskConfig>
)