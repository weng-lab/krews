package krews.config

data class LocalExecConfig (
    val docker: DockerConfig? = DockerConfig(),
    val localBaseDir: String
)

data class DockerConfig (
    val uri: String? = null,
    val certificatesPath: String? = null,
    val connectTimeout: Long = 5_000,
    val readTimeout: Long = 30_000,
    val connectionPoolSize: Int = 100
)
