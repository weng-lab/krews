package krews.config

data class TaskConfig (
    // Task level input parameters
    val params: Map<String, Any> = mapOf(),
    // Task level Google executor configuration
    val google: GoogleTaskConfig? = null,
    // Task level Slurm executor configuration
    val slurm: SlurmTaskConfig? = null,
    // The maximum allowed parallelism for this task
    val parallelism: Parallelism = UnlimitedParallelism
)

data class WorkflowConfig (
    // Workflow level input parameters
    val params: Map<String, Any> = mapOf(),
    // Local executor configuration
    val local: LocalWorkflowConfig = LocalWorkflowConfig(),
    // Google executor configuration
    val google: GoogleWorkflowConfig? = null,
    // Slurm executor configuration
    val slurm: SlurmWorkflowConfig? = null,
    // Configuration for individual tasks
    val tasks: Map<String, TaskConfig>,
    // The maximum allowed parallelism for the system as a whole. Also see per-task parallelism
    val parallelism: Parallelism = UnlimitedParallelism,
    // The concurrency for task setup, status checking, and cleanup
    val executorConcurrency: Int = 16,
    // Delay between uploading the latest database to project working storage
    val dbUploadDelay: Long = 60,
    // Delay between generating updated status reports (in seconds)
    val reportGenerationDelay: Long = 60
)