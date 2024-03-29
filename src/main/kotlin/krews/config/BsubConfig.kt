package krews.config

import krews.core.Capacity

data class BsubWorkflowConfig(
    // Interval in seconds between checks for pipeline job completion
    val jobCompletionPollInterval: Int = 10,
    // Optional ssh configuration. Will cause all slurm command to be passed through ssh.
    // Only available for password-less login.
    val ssh: SshConfig? = null
)

data class BsubTaskConfig(
    // Number of cpus. Can be used to override the runtime value.
    val cpus: Int? = null,
    // Whether or not to request a GPU.
    val gpu: Boolean? = null,
    // Memory capacity. Can be used to override the runtime value.
    val mem: String? = null,
    // Time limit on the run time for the job in minutes.
    val time: String? = null,
    // SBatch partition to use.
    val partition: String? = null,
    // Additional bsub arguments
    val sbatchArgs: Map<String, String>? = null,
    // Additional rusage parameters
    val rUsage: String? = null
)
