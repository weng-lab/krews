package krews.config

data class SingularityConfig (
    val localImage: String? = null
)

data class TaskConfig (
    // Task level input parameters
    val params: Map<String, Any> = mapOf(),
    // Task level Google executor configuration
    val google: GoogleTaskConfig? = null,
    // Task level Slurm executor configuration
    val slurm: SlurmTaskConfig? = null,
    // Task level Slurm executor configuration
    val bsub: BsubTaskConfig? = null,
    // The maximum allowed parallelism for this task
    val parallelism: Parallelism = UnlimitedParallelism,
    // The number of tasks "executions" that will be run with the same job / vm.
    val grouping: Int = 1,
    // Docker image; defaults to one provided by task
    val dockerImage: String? = null,
    // optional Singularity config
    val singularity: SingularityConfig? = null
)

data class WorkflowConfig (
    // Workflow level input parameters
    val params: Map<String, Any> = mapOf(),
    // Working Directory
    val workingDir: String,
    // Local executor configuration
    val local: LocalWorkflowConfig? = null,
    // Google executor configuration
    val google: GoogleWorkflowConfig? = null,
    // Slurm executor configuration
    val slurm: SlurmWorkflowConfig? = null,
    // BSUB executor configuration
    val bsub: BsubWorkflowConfig? = null,
    // The maximum allowed parallelism for the system as a whole. Also see per-task parallelism
    val parallelism: Parallelism = UnlimitedParallelism,
    // The concurrency for task setup, status checking, and cleanup
    val executorConcurrency: Int = 16,
    // Delay between generating updated status reports (in seconds)
    val reportGenerationDelay: Long = 120,
    // Force task runs to run even if all output files exist
    val forceRuns: Boolean = false
)