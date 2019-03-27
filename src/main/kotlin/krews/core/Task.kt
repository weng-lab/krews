package krews.core

import krews.config.convertConfigMap
import krews.file.File
import mu.KotlinLogging
import java.time.Duration

const val DEFAULT_DOCKER_DATA_DIR = "/data"

private val log = KotlinLogging.logger {}

class Task<I : Any, O : Any> @PublishedApi internal constructor(
    val name: String,
    val inputs: List<I>,
    val labels: List<String> = listOf(),
    private val taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit
) {
    fun createTaskRunContexts(rawTaskParams: Map<String, Any>): List<TaskRunContext<I, O>> = inputs.map { input ->
        val taskRunContextBuilder = TaskRunContextBuilder<I, O>(name, input, rawTaskParams)
        taskRunContextBuilder.taskRunContextInit()
        taskRunContextBuilder.build()
    }
}

data class TaskRunContext<I: Any, O: Any>(
    val taskName: String,
    val dockerImage: String,
    val dockerDataDir: String,
    val input: I,
    val output: O,
    val command: String?,
    val env: Map<String, String>?,
    val cpus: Int?,
    val memory: Capacity?,
    val diskSize: Capacity?,
    val time: Duration?,
    val taskParams: Any?,
    val taskParamsClass: Class<*>?
)

class TaskRunContextBuilder<I : Any, O : Any> internal constructor(
    private val taskName: String,
    val input: I,
    @PublishedApi internal val rawTaskParams: Map<String, Any>
) {
    var dockerImage: String? = null
    var dockerDataDir: String = DEFAULT_DOCKER_DATA_DIR
    var command: String? = null
    var output: O? = null
    var env: Map<String, String>? = null
    var cpus: Int? = null
    var memory: Capacity? = null
    var diskSize: Capacity? = null
    var time: Duration? = null
    @PublishedApi internal var taskParams: Any? = null
    @PublishedApi internal var taskParamsClass: Class<*>? = null

    inline fun <reified P : Any> taskParams(): P {
        if (taskParams == null || taskParams !is P) {
            taskParams = convertConfigMap<P>(rawTaskParams)
            taskParamsClass = P::class.java
        }
        return taskParams as P
    }

    val File.dockerPath: String get() = "$dockerDataDir/${this.path}"

    internal fun build(): TaskRunContext<I, O> = TaskRunContext(
        taskName = taskName,
        dockerImage = checkNotNull(dockerImage),
        dockerDataDir = dockerDataDir,
        input = input,
        output = checkNotNull(output),
        command = command?.trimIndent(),
        env = env,
        cpus = cpus,
        memory = memory,
        diskSize = diskSize,
        time = time,
        taskParams = taskParams,
        taskParamsClass = taskParamsClass
    )
}