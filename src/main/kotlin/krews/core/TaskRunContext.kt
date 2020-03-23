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
    var inputsDir: String = DEFAULT_DOCKER_INPUTS_DIR
    var outputsDir: String = DEFAULT_DOCKER_OUTPUTS_DIR
    var command: String? = null
    var output: O? = null
    var env: Map<String, String>? = null
    var cpus: Int? = null
    var memory: Capacity? = null
    var diskSize: Capacity? = null
    var time: Duration? = null
    @PublishedApi internal var taskParams: Any? = null
    @PublishedApi internal var taskParamsClass: Class<*>? = null
    private val outputFilesIn: Set<OutputFile> = getOutputFilesForObject(input)

    inline fun <reified P : Any> taskParams(): P {
        if (taskParams == null || taskParams !is P) {
            taskParams = convertConfigMap<P>(rawTaskParams)
            taskParamsClass = P::class.java
        }
        return taskParams as P
    }

    val File.dockerPath: String get() {
        return when {
            this is InputFile -> "$inputsDir/${this.path}"
            this is OutputFile -> if (outputFilesIn.contains(this)) {
                "$inputsDir/${this.path}"
            } else {
                "$outputsDir/${this.path}"
            }
            else -> throw Exception("Unknown file type!")
        }
    }

    val OutputDirectory.dockerPath: String get() {
        return "$outputsDir/${this.path}"
    }

    internal fun build(): TaskRunContext<I, O> {
        val inputFiles = getInputFilesForObject(input) + getInputFilesForObject(taskParams)
        val outputFilesOut = getOutputFilesForObject(output)
        val outputDirectoriesOut = getOutputDirectoriesForObject(output)

        if (inputsDir == outputsDir) {
            throw Exception("The inputsDir and outputsDir cannot be the same due to container limitations.")
        }

        return TaskRunContext(
            taskName = taskName,
            dockerImage = checkNotNull(dockerImage),
            inputsDir = inputsDir,
            outputsDir = outputsDir,
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
            taskParamsClass = taskParamsClass,
            outputFilesIn = outputFilesIn,
            outputFilesOut = outputFilesOut,
            outputDirectoriesOut = outputDirectoriesOut,
            inputFiles = inputFiles
        )
    }
}

data class TaskRunContext<I: Any, O: Any>(
    val taskName: String,
    val dockerImage: String,
    val inputsDir: String,
    val outputsDir: String,
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
    val taskParamsClass: Class<*>?,
    val outputFilesIn: Set<OutputFile>,
    val outputFilesOut: Set<OutputFile>,
    val outputDirectoriesOut: Set<OutputDirectory>,
    val inputFiles: Set<InputFile>
)
