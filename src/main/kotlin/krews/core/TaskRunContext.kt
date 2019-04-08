package krews.core

import krews.config.convertConfigMap
import krews.file.*
import java.time.Duration

class TaskRunContextBuilder<I : Any, O : Any> internal constructor(
    val taskName: String,
    val input: I,
    @PublishedApi internal val rawTaskParams: Map<String, Any>,
    val inputClass: Class<I>,
    val outputClass: Class<O>
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
        inputClass = inputClass,
        output = checkNotNull(output),
        outputClass = outputClass,
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

data class TaskRunContext<I: Any, O: Any>(
    val taskName: String,
    val dockerImage: String,
    val dockerDataDir: String,
    val input: I,
    val inputClass: Class<I>,
    val output: O,
    val outputClass: Class<O>,
    val command: String?,
    val env: Map<String, String>?,
    val cpus: Int?,
    val memory: Capacity?,
    val diskSize: Capacity?,
    val time: Duration?,
    val taskParams: Any?,
    val taskParamsClass: Class<*>?
) {
    val outputFilesIn by lazy { getOutputFilesForObject(input) }
    val outputFilesOut by lazy { getOutputFilesForObject(output) }
    val inputFiles by lazy { getInputFilesForObject(input) + getInputFilesForObject(taskParams) }
}
