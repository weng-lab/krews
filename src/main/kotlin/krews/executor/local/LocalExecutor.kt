package krews.executor.local

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.InspectContainerResponse
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
import krews.core.TaskRunner
import krews.executor.*
import krews.file.*
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

private val log = KotlinLogging.logger {}

class LocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.local?.docker ?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.localFilesBaseDir).toAbsolutePath()!!
    private val inputsPath = workflowBasePath.resolve(INPUTS_DIR)
    private val outputsPath = workflowBasePath.resolve(OUTPUTS_DIR)

    private val runningContainers: MutableSet<String> = ConcurrentHashMap.newKeySet<String>()

    private var allShutdown = false

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

    override fun downloadInputFile(inputFile: InputFile) = downloadInputFileLocalFS(inputFile, inputsPath)

    private inner class DockerJob(
        val containerId: String,
        val dockerClient: DockerClient,
        val logBasePath: Path,
        val runBasePath: Path,
        val mountDir: Path,
        val taskRunId: Int,
        val outputFilesOut: Set<OutputFile>) : Future<Unit> {
        private var lastState: InspectContainerResponse.ContainerState? = null
        private var capturedThrowable: Throwable? = null
        private var cancelled = false
        private var cleaned = false

        private fun checkStatus() {
            if (lastState?.running == false) {
                return
            }
            try {
                val inspect = dockerClient.inspectContainerCmd(containerId).exec()
                lastState = inspect.state
            } catch (e: Throwable) {
                capturedThrowable = e
            }
        }

        private fun onFinished(success: Boolean) {
            if (cleaned) {
                return
            }
            cleaned = true
            try {
                runningContainers.remove(containerId)
                copyLogsFromContainer(dockerClient, containerId, logBasePath)

                // Delete containers
                log.info { "Cleaning up containers..." }
                dockerClient.removeContainerCmd(containerId).exec()

                if (success) {
                    // Copy output files out of docker container into run outputs dir
                    if (outputFilesOut.isNotEmpty()) {
                        log.info { "Copying output files $outputFilesOut for task output out of mounted data dir $mountDir" }
                    } else {
                        log.info { "No output files to copy for this task run." }
                    }
                    outputFilesOut.map { it.path }.forEach {
                        val to = outputsPath.resolve(it)
                        Files.createDirectories(to.parent)
                        Files.copy(mountDir.resolve(it), to, StandardCopyOption.REPLACE_EXISTING)
                    }
                } else {
                    // Copy all files in mounted docker data directory to task diagnostics directory
                    val taskDiagnosticsDir = runBasePath.resolve(DIAGNOSTICS_DIR).resolve(taskRunId.toString())
                    copyDiagnosticFiles(mountDir, taskDiagnosticsDir)
                }
            } finally {
                // Clean up temporary mount dir
                Files.walk(mountDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach { Files.delete(it) }
            }
        }

        override fun isDone(): Boolean {
            checkStatus()
            return capturedThrowable != null || lastState?.running == false
        }

        override fun get() {
            // An arbitrarily high value that won't overflow the long
            return get(30, TimeUnit.DAYS)
        }

        override fun get(timeout: Long, unit: TimeUnit?) {
            try {
                if (capturedThrowable != null) {
                    throw capturedThrowable!!
                }
                if (lastState?.running != false) {
                    dockerClient.waitContainerCmd(containerId).exec(WaitContainerResultCallback())
                        .awaitStatusCode(timeout, unit)
                }
                val statusCode = lastState!!.exitCode!!
                checkStatus()
                if (lastState?.running == true) {
                    throw Exception("Expected container to not be running.")
                }
                if (statusCode > 0) {
                    val taskDiagnosticsDir = runBasePath.resolve(DIAGNOSTICS_DIR).resolve(taskRunId.toString())
                    throw Exception(
                        """
                |Container exited with code $statusCode.
                |Please see logs at $logBasePath for more information.
                |Working files (in data directory) have been copied into $taskDiagnosticsDir
                """.trimMargin()
                    )
                }
            } finally {
                onFinished(lastState?.exitCode == 0)
            }
        }


        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            if (!mayInterruptIfRunning && !isDone) {
                return false
            }
            try {
                log.info { "Stopping container $containerId..." }
                dockerClient.stopContainerCmd(containerId).exec()
            } finally {
                checkStatus()
            }
            cancelled = true
            return true
        }

        override fun isCancelled(): Boolean {
            return allShutdown || cancelled
        }
    }

    override fun executeTask(
        workflowRunDir: String,
        taskRunId: Int,
        taskConfig: TaskConfig,
        taskRunContext: TaskRunContext<*, *>,
        outputFilesIn: Set<OutputFile>,
        outputFilesOut: Set<OutputFile>,
        cachedInputFiles: Set<InputFile>,
        downloadInputFiles: Set<InputFile>
    ): Future<Unit> {
        if (allShutdown) {
            throw Exception("shutdownRunningTasks has already been called")
        }

        // Create a temp directory to use as a mount for input data
        val mountDir = workflowBasePath.resolve("task-$taskRunId-mount")
        Files.createDirectories(mountDir)

        try {
            val runBasePath = workflowBasePath.resolve(workflowRunDir)

            // Pull image from remote
            log.info { "Pulling image \"${taskRunContext.dockerImage}\" from remote..." }
            dockerClient.pullImageCmd(taskRunContext.dockerImage).exec(PullImageResultCallback()).awaitSuccess()

            // Download InputFiles from remote sources
            for (downloadInputFile in downloadInputFiles) {
                downloadRemoteInputFile(dockerClient, downloadInputFile, taskRunContext.dockerDataDir, mountDir)
            }

            // Create the task execution docker container from config
            val containerId = createContainer(dockerClient, taskRunContext, mountDir)

            // Copy cachedInputFiles into the docker container
            for (cachedInputFile in cachedInputFiles) {
                Files.copy(
                    Paths.get(workflowBasePath.toString(), INPUTS_DIR, cachedInputFile.path),
                    mountDir.resolve(cachedInputFile.path)
                )
            }

            // Copy OutputFiles from task input into the docker container
            if (!outputFilesIn.isEmpty()) {
                log.info { "Copying output files $outputFilesIn from task input into newly created container $containerId" }
            }
            for (outputFile in outputFilesIn) {
                val toPath = mountDir.resolve(outputFile.path)
                Files.createDirectories(toPath.parent)
                Files.copy(outputsPath.resolve(outputFile.path), toPath)
            }

            // Start the container and wait for it to finish processing
            log.info { "Starting container $containerId..." }
            dockerClient.startContainerCmd(containerId).exec()
            runningContainers.add(containerId)

            val logBasePath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())

            return DockerJob(containerId, dockerClient, logBasePath, runBasePath, mountDir, taskRunId, outputFilesOut)
        } catch(e: Throwable) {
            // Clean up temporary mount dir
            Files.walk(mountDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.delete(it) }
            return CompletableFuture.failedFuture(e)
        }
    }

    override fun shutdownRunningTasks() {
        allShutdown = true
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
    if (taskRunContext.env?.isNotEmpty() == true) containerCreationCmd.withEnv(taskRunContext.env.map { "${it.key}=${it.value}" })
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