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
import krews.core.TaskRunContext
import krews.executor.*
import krews.file.InputFile
import krews.file.LocalInputFile
import krews.file.OutputFile
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Collectors.toSet
import kotlin.streams.toList

private val log = KotlinLogging.logger {}

class LocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.local?.docker ?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.localFilesBaseDir).toAbsolutePath()!!

    override fun downloadFile(path: String) {}
    override fun uploadFile(path: String) {}

    override fun fileExists(path: String) = Files.exists(Paths.get(path))
    override fun fileLastModified(path: String) = Files.getLastModifiedTime(Paths.get(path)).toMillis()

    override fun executeTask(workflowRunDir: String,
                             taskRunId: Int,
                             taskConfig: TaskConfig,
                             taskRunContext: TaskRunContext<*, *>,
                             outputFilesIn: Set<OutputFile>,
                             outputFilesOut: Set<OutputFile>,
                             cachedInputFiles: Set<InputFile>,
                             downloadInputFiles: Set<InputFile>) {
        val runBasePath = workflowBasePath.resolve(workflowRunDir)
        val runInputsPath = runBasePath.resolve(INPUTS_DIR)
        val runOutputsPath = runBasePath.resolve(OUTPUTS_DIR)

        // Pull image from remote
        log.info { "Pulling image \"${taskRunContext.dockerImage}\" from remote..." }
        dockerClient.pullImageCmd(taskRunContext.dockerImage).exec(PullImageResultCallback()).awaitSuccess()

        // Create a temp directory to use as a mount for input data
        val mountDir = workflowBasePath.resolve("task-$taskRunId-mount")
        Files.createDirectories(mountDir)

        // Download InputFiles from remote sources
        for (downloadInputFile in downloadInputFiles) {
            downloadRemoteInputFile(dockerClient, downloadInputFile, taskRunContext.dockerDataDir, mountDir)
        }

        // Create the task execution docker container from config
        log.info { "Creating container from image \"${taskRunContext.dockerImage}\" with mount $mountDir" }
        val volume = Volume(taskRunContext.dockerDataDir)
        val containerCreationCmd = dockerClient.createContainerCmd(taskRunContext.dockerImage)
            .withVolumes(volume)
            .withBinds(Bind(mountDir.toString(), volume))
        if (taskRunContext.command != null) containerCreationCmd.withCmd("/bin/sh", "-c", taskRunContext.command)
        if (taskRunContext.env.isNotEmpty()) containerCreationCmd.withEnv(taskRunContext.env.map { "${it.key}=${it.value}" })
        val createContainerResponse = containerCreationCmd.exec()
        val containerId = createContainerResponse.id!!

        // Copy localInputFiles into the docker container
        for (cachedInputFile in cachedInputFiles) {
            Files.copy(Paths.get(workflowBasePath.toString(), INPUTS_DIR, cachedInputFile.path),
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
                // Copy all files in mounted docker data directory to task diagnostics directory
                val taskDiagnosticsDir = runBasePath.resolve(DIAGNOSTICS_DIR).resolve(taskRunId.toString())
                Files.createDirectories(taskDiagnosticsDir)
                Files.walk(mountDir)
                    .forEach { source -> Files.copy(source, taskDiagnosticsDir.resolve(source.relativize(mountDir))) }
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

    override fun downloadInputFile(inputFile: InputFile) {
        if (inputFile is LocalInputFile) {
            Files.copy(Paths.get(inputFile.localPath), workflowBasePath.resolve(inputFile.path))
            return
        }
        inputFile.downloadLocal(workflowBasePath)
    }

    override fun listFiles(baseDir: String): Set<String> {
        return Files.walk(workflowBasePath.resolve(baseDir))
            .filter { Files.isRegularFile(it) }
            .map { it.toString() }
            .toList().toSet()
    }

    override fun deleteFile(file: String){
        Files.delete(workflowBasePath.resolve(file))
    }

}
