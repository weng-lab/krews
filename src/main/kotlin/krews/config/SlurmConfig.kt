package krews.config

import krews.core.Capacity

data class SlurmWorkflowConfig(
    val sshConfig: SshConfig? = null
)

data class SshConfig(
    val user: String,
    val host: String,
    val port: Int = 22
)

data class SlurmTaskConfig(
    // Number of cpus. Can be used to override the runtime value.
    val cpus: Int? = null,
    // Memory capacity. Can be used to override the runtime value.
    val mem: Capacity? = null,
    // Time limit on the run time for the job in minutes.
    val time: Int? = null,
    // SBatch partition to use.
    val partition: String? = null
)