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
    var outputsDir: String = DEFAULT_DOCKER_DATA_DIR
    var dockerDataFilesDir: String = "${DEFAULT_DOCKER_DATA_DIR}_files"
    var command: String? = null
    var output: O? = null
    var env: Map<String, String>? = null
    var cpus: Int? = null
    var memory: Capacity? = null
    var diskSize: Capacity? = null
    var time: Duration? = null
    @PublishedApi internal var taskParams: Any? = null
    @PublishedApi internal var taskParamsClass: Class<*>? = null
    val inputFiles = mutableSetOf<InputFile>()
    val outputFiles = mutableSetOf<OutputFile>()

    inline fun <reified P : Any> taskParams(): P {
        if (taskParams == null || taskParams !is P) {
            taskParams = convertConfigMap<P>(rawTaskParams)
            taskParamsClass = P::class.java
        }
        return taskParams as P
    }

    val File.dockerPath: String get() {
        return when {
            this is InputFile -> "$dockerDataFilesDir/${this.path}"
            this is OutputFile -> if (this.createdTaskName == taskName) {
                "$outputsDir/${this.path}"
            } else {
                "$dockerDataFilesDir/${this.path}"
            }
            else -> throw Exception("Unknown file type!")
        }
    }

    fun outputFile(path: String): OutputFile {
        val file = OutputFile(path, taskName)
        outputFiles.add(file)
        return file
    }

    fun localInputFile(localPath: String, path: String = localPath): InputFile {
        val file = LocalInputFile(localPath, path)
        inputFiles.add(file)
        return file
    }

    internal fun build(): TaskRunContext<I, O> {
        val outputFilesIn = getOutputFilesForObject(input)
        val outputFilesOut = getOutputFilesForObject(output) + outputFiles
        val inputFiles = getInputFilesForObject(input) + getInputFilesForObject(taskParams) + inputFiles

        if (dockerDataFilesDir == outputsDir) {
            throw Exception("The dockerDataFilesDir and outputsDir cannot be the same due to container limitations.")
        }

        return TaskRunContext(
            taskName = taskName,
            dockerImage = checkNotNull(dockerImage),
            outputsDir = outputsDir,
            dockerDataFilesDir = dockerDataFilesDir,
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
            inputFiles = inputFiles
        )
    }
}

data class TaskRunContext<I: Any, O: Any>(
    val taskName: String,
    val dockerImage: String,
    val outputsDir: String,
    val dockerDataFilesDir: String,
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
    val inputFiles: Set<InputFile>
)
