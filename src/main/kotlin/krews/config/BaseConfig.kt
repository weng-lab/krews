package krews.config

data class TaskConfig (
    // Task level input parameters
    val params: Map<String, Any> = mapOf(),
    // Task level Google executor configuration
    val google: GoogleTaskConfig? = null,
    // The maximum allowed parallelism for this task
    val parallelism: Parallelism = UnlimitedParallelism
)

data class WorkflowConfig (
    // Workflow level input parameters
    val params: Map<String, Any> = mapOf(),
    // Directory where files handled locally are kept. For the local Executor, all output goes here
    val localFilesBaseDir: String = "workflow-out",
    // Local executor configuration
    val local: LocalWorkflowConfig? = null,
    // Google executor configuration
    val google: GoogleWorkflowConfig? = null,
    // Configuration for individual tasks
    val tasks: Map<String, TaskConfig>,
    // The maximum allowed parallelism for the system as a whole. Also see per-task parallelism
    val parallelism: Parallelism = UnlimitedParallelism,
    // Delete old runs from db and file storage (run dirs) when the current run succeeds
    val cleanOldRuns: Boolean = true,
    // Delay between generating updated status reports (in seconds)
    val reportGenerationDelay: Long = 60
)