package krews.config

data class LocalWorkflowConfig (
    val workingDir: String = "workflow-out",
    val docker: DockerConfig? = DockerConfig()
)

data class DockerConfig (
    val uri: String? = null,
    val certificatesPath: String? = null,
    val connectTimeout: Int = 5_000,
    val readTimeout: Int = 30_000,
    val connectionPoolSize: Int = 100
)
