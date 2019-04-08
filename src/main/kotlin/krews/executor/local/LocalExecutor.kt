package krews.executor.local

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.*
import com.github.dockerjava.core.command.*
import kotlinx.coroutines.delay
import krews.config.*
import krews.core.*
import krews.executor.*
import krews.file.*
import mu.KotlinLogging
import java.nio.file.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val log = KotlinLogging.logger {}

@Suppress("BlockingMethodInNonBlockingContext")
class LocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.local?.docker ?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.workingDir).toAbsolutePath()!!
    private val outputsPath = workflowBasePath.resolve(OUTPUTS_DIR)

    private val runningContainers: MutableSet<String> = ConcurrentHashMap.newKeySet<String>()

    private val allShutdown = AtomicBoolean(false)

    // Disable grouping because it's meaningless for local executions
    // This will automatically force executeTask to be called with one TaskRunContext at a time
    override fun supportsGrouping() = false

    override fun downloadFile(fromPath: String, toPath: Path) {
        val fromFile = workflowBasePath.resolve(fromPath)
        log.info { "Attempting to copy $fromFile to $toPath..." }
        val fileExists = Files.exists(fromFile)
        if (fileExists) {
            Files.createDirectories(toPath.parent)
            Files.copy(fromFile, toPath, StandardCopyOption.REPLACE_EXISTING)
            log.info { "$fromFile successfully copied to $toPath!" }
        } else {
            log.info { "$fromFile not found. It will not be copied." }
        }
    }
    override fun uploadFile(fromPath: Path, toPath: String, backup: Boolean) {
        val toFile = workflowBasePath.resolve(toPath)
        log.info { "Copying file $fromPath to $toFile" }
        Files.createDirectories(toFile.parent)
        Files.copy(fromPath, toFile, StandardCopyOption.REPLACE_EXISTING)

        if (backup) {
            val backupFile = workflowBasePath.resolve("$toFile.backup")
            log.info { "Backing up file $toFile to $backupFile" }
            Files.copy(toFile, backupFile, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    override fun fileExists(path: String) = Files.exists(workflowBasePath.resolve(path))
    override fun fileLastModified(path: String) = Files.getLastModifiedTime(workflowBasePath.resolve(path)).toMillis()

    override fun listFiles(baseDir: String): Set<String> = listLocalFiles(workflowBasePath.resolve(baseDir))
    override fun deleteFile(file: String) = Files.delete(workflowBasePath.resolve(file))

    override suspend fun executeTask(
        workflowRunDir: String,
        taskRunId: Int,
        taskConfig: TaskConfig,
        taskRunContexts: List<TaskRunContext<*, *>>
    ) {
        if (allShutdown.get()) {
            throw Exception("shutdownRunningTasks has already been called")
        }
        if (taskRunContexts.size > 1) {
            throw Exception("Local Executor does not handle grouping.")
        }
        val taskRunContext = taskRunContexts.first()

        // Create a temp directory to use as a mount for input data
        val mountDir = workflowBasePath.resolve("task-$taskRunId-mount")
        Files.createDirectories(mountDir)

        try {
            val runBasePath = workflowBasePath.resolve(workflowRunDir)

            // Pull image from remote
            log.info { "Pulling image \"${taskRunContext.dockerImage}\" from remote..." }
            dockerClient.pullImageCmd(taskRunContext.dockerImage).exec(PullImageResultCallback()).awaitSuccess()

            // Download InputFiles from remote sources
            for (downloadInputFile in taskRunContext.inputFiles) {
                downloadRemoteInputFile(dockerClient, downloadInputFile, taskRunContext.dockerDataDir, mountDir)
            }

            // Create the task execution docker container from config
            val containerId = createContainer(dockerClient, taskRunContext, mountDir)

            // Copy OutputFiles from task input into the docker container
            if (!taskRunContext.outputFilesIn.isEmpty()) {
                log.info { "Copying output files ${taskRunContext.outputFilesIn} from task input into newly " +
                        "created container $containerId" }
            }
            for (outputFile in taskRunContext.outputFilesIn) {
                val toPath = mountDir.resolve(outputFile.path)
                Files.createDirectories(toPath.parent)
                Files.copy(outputsPath.resolve(outputFile.path), toPath)
            }

            // Start the container and wait for it to finish processing
            log.info { "Starting container $containerId..." }
            dockerClient.startContainerCmd(containerId).exec()
            runningContainers.add(containerId)

            val logBasePath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())

            try {
                while (true) {
                    val inspect = dockerClient.inspectContainerCmd(containerId).exec()
                    if (inspect.state.running == true) {
                        delay(1000)
                        continue
                    }

                    log.info { "Waiting for container $containerId to finish..." }
                    val statusCode =
                        dockerClient.waitContainerCmd(containerId).exec(WaitContainerResultCallback()).awaitStatusCode()
                    runningContainers.remove(containerId)
                    if (statusCode > 0) {
                        // Copy all files in mounted docker data directory to task diagnostics directory
                        val taskDiagnosticsDir = runBasePath.resolve(DIAGNOSTICS_DIR).resolve(taskRunId.toString())
                        copyDiagnosticFiles(mountDir, taskDiagnosticsDir)
                        throw Exception(
                            """
                            |Container exited with code $statusCode.
                            |Please see logs at $logBasePath for more information.
                            |Working files (in data directory) have been copied into $taskDiagnosticsDir
                            """.trimMargin()
                        )
                    }
                    break
                }
                log.info { "Container $containerId finished successfully!" }
            } finally {
                copyLogsFromContainer(dockerClient, containerId, logBasePath)

                // Delete containers
                log.info { "Cleaning up containers..." }
                dockerClient.removeContainerCmd(containerId).exec()
            }

            // Copy output files out of docker container into run outputs dir
            if (taskRunContext.outputFilesOut.isNotEmpty()) {
                log.info { "Copying output files ${taskRunContext.outputFilesOut} for task output out of mounted " +
                        "data dir $mountDir" }
            } else {
                log.info { "No output files to copy for this task run." }
            }
            taskRunContext.outputFilesOut.map { it.path }.forEach {
                val to = outputsPath.resolve(it)
                Files.createDirectories(to.parent)
                Files.copy(mountDir.resolve(it), to, StandardCopyOption.REPLACE_EXISTING)
            }
        } finally {
            // Clean up temporary mount dir
            Files.walk(mountDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.delete(it) }
        }
    }

    override fun shutdownRunningTasks() {
        allShutdown.set(true)
        for(containerId in runningContainers) {
            log.info { "Stopping container $containerId..." }
            dockerClient.stopContainerCmd(containerId).exec()
        }
    }

}

fun createContainer(dockerClient: DockerClient, taskRunContext: TaskRunContext<*, *>, mountDir: Path): String {
    log.info { "Creating container from image \"${taskRunContext.dockerImage}\" with mount $mountDir" }
    val volume = Volume(taskRunContext.dockerDataDir)
    val containerCreationCmd = dockerClient.createContainerCmd(taskRunContext.dockerImage)
        .withVolumes(volume)
        .withBinds(Bind(mountDir.toString(), volume))
    if (taskRunContext.command != null) containerCreationCmd.withCmd("/bin/sh", "-c", taskRunContext.command)
    if (taskRunContext.env?.isNotEmpty() == true)
        containerCreationCmd.withEnv(taskRunContext.env.map { "${it.key}=${it.value}" })
    val createContainerResponse = containerCreationCmd.exec()
    return createContainerResponse.id!!
}

private fun copyDiagnosticFiles(mountDir: Path, taskDiagnosticsDir: Path) {
    Files.createDirectories(taskDiagnosticsDir)
    Files.walk(mountDir)
        .filter { source -> !Files.exists(taskDiagnosticsDir.resolve(source.relativize(mountDir))) }
        .forEach { source ->
            Files.copy(
                source,
                taskDiagnosticsDir.resolve(source.relativize(mountDir))
            )
        }
}

private fun copyLogsFromContainer(dockerClient: DockerClient, containerId: String, logBasePath: Path) {
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