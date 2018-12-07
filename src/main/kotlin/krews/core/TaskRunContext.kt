package krews.core

import krews.config.convertConfigMap
import krews.file.File
import krews.misc.mapper
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class TaskRunContextBuilder<I : Any, O : Any> internal constructor(
    val input: I,
    @PublishedApi internal val rawTaskParams: Map<String, Any>
) {
    var dockerImage: String? = null
    var dockerDataDir: String = DEFAULT_DOCKER_DATA_DIR
    var command: String? = null
    var output: O? = null
    var env: MutableMap<String, String> = mutableMapOf()
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
        dockerImage = checkNotNull(dockerImage),
        dockerDataDir = dockerDataDir,
        input = input,
        output = checkNotNull(output),
        command = command?.trimIndent(),
        env = env,
        taskParams = taskParams,
        taskParamsClass = taskParamsClass
    )
}

data class TaskRunContext<I: Any, O: Any>(
    val dockerImage: String,
    val dockerDataDir: String,
    val input: I,
    val output: O,
    val command: String?,
    val env: Map<String, String>,
    val taskParams: Any?,
    val taskParamsClass: Class<*>?
)
