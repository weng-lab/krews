package krews.config

data class TaskConfig (
    val params: Map<String, Any>? = mapOf(),
    val env: Map<String, String>? = null,
    val google: GoogleTaskConfig? = null,
    val parallelism: Parallelism = UnlimitedParallelism
)

data class WorkflowConfig (
    val params: Map<String, Any> = mapOf(),
    val local: LocalWorkflowConfig? = null,
    val google: GoogleWorkflowConfig? = null,
    val tasks: Map<String, TaskConfig>,
    val parallelism: Parallelism = UnlimitedParallelism
)