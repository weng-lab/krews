package krews.executor.local

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.StreamType
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.core.command.LogContainerResultCallback
import com.github.dockerjava.core.command.PullImageResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback
import krews.config.DockerConfig
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.executor.*
import krews.file.InputFile
import krews.file.OutputFile
import mu.KotlinLogging
import org.joda.time.DateTime
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*


const val DEFAULT_LOCAL_BASE_DIR = "workflow-out"

private val log = KotlinLogging.logger {}

class LocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.local?.docker ?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.local?.localBaseDir ?: DEFAULT_LOCAL_BASE_DIR).toAbsolutePath()!!

    override fun prepareDatabaseFile(): String {
        val statePath = workflowBasePath.resolve(STATE_DIR)
        Files.createDirectories(statePath)
        return statePath.resolve(DB_FILENAME).toString()
    }

    override fun pushDatabaseFile() {}

    override fun outputFileLastModified(runOutputsDir: String, outputFile: OutputFile): Long {
        val filePath = Paths.get(workflowBasePath.toString(), runOutputsDir, outputFile.path)
        return Files.getLastModifiedTime(filePath).toMillis()
    }

    override fun copyCachedFiles(fromDir: String, toDir: String, files: Set<String>) {
        val fromBasePath = Paths.get(workflowBasePath.toString(), fromDir)
        val toBasePath = Paths.get(workflowBasePath.toString(), toDir)
        for (file in files) {
            val fromPath = fromBasePath.resolve(file)
            val toPath = toBasePath.resolve(file)
            log.info { "Copying cached file from $fromPath to $toPath" }
            Files.createDirectories(toPath.parent)
            Files.copy(fromPath, toPath)
            val fromLastModified = Files.getLastModifiedTime(fromPath)
            Files.setLastModifiedTime(toPath, fromLastModified)
        }
    }

    override fun executeTask(workflowRunDir: String, taskRunId: Int, taskConfig: TaskConfig, dockerImage: String,
                             dockerDataDir: String, command: String?, outputFilesIn: Set<OutputFile>, outputFilesOut: Set<OutputFile>,
                             cachedInputFiles: Set<CachedInputFile>, downloadInputFiles: Set<InputFile>) {
        val runBasePath = Paths.get(workflowBasePath.toString(), workflowRunDir)
        val runInputsPath = runBasePath.resolve(INPUTS_DIR)
        val runOutputsPath = runBasePath.resolve(OUTPUTS_DIR)

        // Pull image from remote
        log.info { "Pulling image \"$dockerImage\" from remote..." }
        dockerClient.pullImageCmd(dockerImage).exec(PullImageResultCallback()).awaitSuccess()

        // Create a temp directory to use as a mount for input data
        val mountDir = workflowBasePath.resolve("task-$taskRunId-mount")
        Files.createDirectories(mountDir)

        // Download InputFiles from remote sources
        for (downloadInputFile in downloadInputFiles) {
            downloadRemoteInputFile(dockerClient, downloadInputFile, dockerDataDir, mountDir)
        }

        // Create the task execution docker container from config
        log.info { "Creating container from image \"$dockerImage\" with mount $mountDir" }
        val volume = Volume(dockerDataDir)
        val containerCreationCmd = dockerClient.createContainerCmd(dockerImage)
            .withVolumes(volume)
            .withBinds(Bind(mountDir.toString(), volume))
        if (command != null) containerCreationCmd.withCmd("/bin/sh", "-c", command)
        if (taskConfig.env != null) containerCreationCmd.withEnv(taskConfig.env.map { "${it.key}=${it.value}" })
        val createContainerResponse = containerCreationCmd.exec()
        val containerId = createContainerResponse.id!!

        // Copy localInputFiles into the docker container
        for (cachedInputFile in cachedInputFiles) {
            Files.copy(Paths.get(workflowBasePath.toString(), cachedInputFile.workflowInputsDir, cachedInputFile.path),
                mountDir.resolve(cachedInputFile.path))
        }

        // Copy OutputFiles from task input into the docker container
        if (!outputFilesIn.isEmpty()) {
            log.info { "Copying output files $outputFilesIn from task input into newly created container $containerId" }
            for (outputFile in outputFilesIn) {
                val toPath = mountDir.resolve(outputFile.path)
                Files.createDirectories(toPath.parent)
                Files.copy(runOutputsPath.resolve(outputFile.path), toPath)
            }
        }

        // Start the container and wait for it to finish processing
        log.info { "Starting container $containerId..." }
        dockerClient.startContainerCmd(containerId).exec()

        val logBasePath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())

        try {
            log.info { "Waiting for container $containerId to finish..." }
            val statusCode = dockerClient.waitContainerCmd(containerId).exec(WaitContainerResultCallback()).awaitStatusCode()
            if (statusCode > 0) {
                throw Exception("Container exited with code $statusCode. Please see logs at $logBasePath for more information.")
            }
            log.info { "Container $containerId finished successfully!" }
        } finally {
            // Copy logs from container
            log.info { "Copying logs from container $containerId" }
            Files.createDirectories(logBasePath)
            val logCallback = object : LogContainerResultCallback() {
                override fun onNext(item: Frame?) {
                    if (item?.streamType == StreamType.STDOUT) {
                        Files.newOutputStream(logBasePath.resolve("stdout.txt")).use { it.write(item.payload) }
                    }
                    if (item?.streamType == StreamType.STDERR) {
                        Files.newOutputStream(logBasePath.resolve("stderr.txt")).use { it.write(item.payload) }
                    }
                }
            }
            dockerClient.logContainerCmd(containerId)
                .withStdOut(true)
                .withStdErr(true)
                .exec(logCallback).awaitCompletion()
        }

        // Copy newly downloaded input files out of docker container into run inputs dir
        if (downloadInputFiles.isNotEmpty()) {
            log.info { "Copying downloaded input files $downloadInputFiles out of mounted data dir $mountDir" }
        }
        downloadInputFiles.map { it.path }.forEach {
            val to = runInputsPath.resolve(it)
            Files.createDirectories(to.parent)
            Files.copy(mountDir.resolve(it), to)
        }

        // Copy output files out of docker container into run outputs dir
        if (outputFilesOut.isNotEmpty()) {
            log.info { "Copying output files $outputFilesOut for task output out of mounted data dir $mountDir" }
        } else {
            log.info { "No output files to copy for this task run." }
        }
        outputFilesOut.map { it.path }.forEach {
            val to = runOutputsPath.resolve(it)
            Files.createDirectories(to.parent)
            Files.copy(mountDir.resolve(it), to)
        }

        // Delete containers
        log.info { "Cleaning up containers..." }
        dockerClient.removeContainerCmd(containerId).exec()

        // Clean up temporary mount dir
        Files.walk(mountDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }

    override fun downloadRemoteInputFiles(inputFiles: Set<InputFile>, dockerDataDir: String, workflowInputsDir: String) {
        val mountDir = workflowBasePath.resolve("download-inputs-${UUID.randomUUID()}")
        val runInputsPath = Paths.get(workflowBasePath.toString(), workflowInputsDir)
        for (inputFile in inputFiles) {
            downloadRemoteInputFile(dockerClient, inputFile, dockerDataDir, mountDir)
            Files.copy(mountDir.resolve(inputFile.path), runInputsPath.resolve(inputFile.path))
        }
        Files.walk(mountDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }

}
