package krews.executor.local

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.StreamType
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.LogContainerResultCallback
import com.github.dockerjava.core.command.PullImageResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import krews.WFile
import krews.config.DockerConfig
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.executor.*
import mu.KotlinLogging
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.io.IOUtils
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.GZIPOutputStream


const val DEFAULT_LOCAL_BASE_DIR = "workflow-out"

private val log = KotlinLogging.logger {}

class LocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.local?.docker ?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.local?.localBaseDir ?: DEFAULT_LOCAL_BASE_DIR).toAbsolutePath()!!

    override fun prepareDatabaseFile(): String {
        Files.createDirectories(workflowBasePath)
        return workflowBasePath.resolve(STATE_DIR).resolve(DB_FILENAME).toString()
    }

    override fun pushDatabaseFile() {}

    override fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<WFile>) {
        val fromBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, fromWorkflowDir, OUTPUTS_DIR)
        val toBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, toWorkflowDir, OUTPUTS_DIR)
        outputFiles.forEach {
            val fromPath = fromBasePath.resolve(it.path)
            val toPath = toBasePath.resolve(it.path)
            log.info { "Copying cached file from $fromPath to $toPath" }
            Files.createDirectories(toPath.parent)
            Files.copy(fromPath, toPath)
        }
    }

    override fun executeTask(workflowRunDir: String, taskRunId: Int, taskConfig: TaskConfig, dockerImage: String,
                             dockerDataDir: String, command: String?, inputItem: Any, outputItem: Any?) {
        val runBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, workflowRunDir)

        // Pull image from remote
        log.info { "Pulling image \"$dockerImage\" from remote..." }
        dockerClient.pullImageCmd(dockerImage).exec(PullImageResultCallback()).awaitSuccess()

        // Create the docker container from config
        log.info { "Creating container from image \"$dockerImage\"" }
        val containerCreationCmd = dockerClient.createContainerCmd(dockerImage)
        if (command != null) containerCreationCmd.withCmd("/bin/sh", "-c", command)
        val createContainerResponse = containerCreationCmd.exec()
        val containerId = createContainerResponse.id!!

        // Copy input files into the docker container
        val inputFiles = getFilesForObject(inputItem)
        if (!inputFiles.isEmpty()) {
            log.info { "Copying input files $inputFiles into newly created container $containerId" }
            copyInputFiles(dockerClient, runBasePath, dockerDataDir, containerId, inputFiles)
        }

        // Start the container and wait for it to finish processing
        log.info { "Starting container $containerId" }
        dockerClient.startContainerCmd(containerId).exec()
        log.info { "Waiting for container $containerId to finish..." }
        dockerClient.waitContainerCmd(containerId).exec(WaitContainerResultCallback()).awaitCompletion()

        // Copy output files out of docker container
        if (outputItem != null) {
            val outputFiles = getFilesForObject(outputItem)
            log.info { "Copying output files $outputFiles out of container $containerId" }
            copyOutputFiles(dockerClient, runBasePath, dockerDataDir, containerId, outputFiles)
        }

        // Copy logs from container
        log.info { "Copying logs from container $containerId" }
        val logBasePath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())
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

}

/**
 * Build a DockerClient from a given DockerConfig
 */
private fun buildDockerClient(config: DockerConfig): DockerClient {
    val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder()
    if (config.uri != null) configBuilder.withDockerHost(config.uri)
    if (config.certificatesPath != null) configBuilder.withDockerCertPath(config.certificatesPath)
    val dockerConfig = configBuilder.build()

    val dockerCmdExecFactory = JerseyDockerCmdExecFactory()
        .withReadTimeout(config.readTimeout)
        .withConnectTimeout(config.connectTimeout)
        .withMaxTotalConnections(config.connectionPoolSize)

    return DockerClientBuilder.getInstance(dockerConfig)
        .withDockerCmdExecFactory(dockerCmdExecFactory)
        .build()
}

/**
 * Copy input files from the local file system into the docker container
 *
 * @param dockerClient: DockerClient used to push the files
 * @param runBasePath: Local file system path where the current run data and therefor input files can be found.
 * @param dockerDataDir: Directory the input files will be copied to in the docker container
 * @param containerId: ID for the container the files will be copied to.
 * @param inputFiles: Input files as WFiles, which contain partial paths only.
 */
private fun copyInputFiles(dockerClient: DockerClient, runBasePath: Path, dockerDataDir: String, containerId: String,
                           inputFiles: Set<WFile>) {
    if (inputFiles.isEmpty()) return
    inputFiles.forEach { inputFile ->
        val tarInputStream = createTarStream(
            runBasePath.resolve(OUTPUTS_DIR).resolve(inputFile.path),
            "$dockerDataDir/${inputFile.path}"
        )
        dockerClient.copyArchiveToContainerCmd(containerId)
            .withTarInputStream(tarInputStream)
            .withRemotePath("/")
            .exec()
    }
}

/**
 * Creates a Tar Archive as an InputStream for the given file.
 *
 * @param basePath: Directory the file can be found in. This will not be part of the path in the tar archive.
 * @param filePath: Partial path for the file that will be archived. This is the only part that will be in the archive.
 */
private fun createTarStream(localPath: Path, dockerPath: String): InputStream {
    val archivePath = Files.createTempFile("docker-client-", ".tar.gz")
    try {
        TarArchiveOutputStream(GZIPOutputStream(BufferedOutputStream(Files.newOutputStream(archivePath)))).use { archiveOutStream ->
            val tarEntry = TarArchiveEntry(dockerPath)
            tarEntry.size = Files.size(localPath)
            archiveOutStream.putArchiveEntry(tarEntry)
            BufferedInputStream(Files.newInputStream(localPath)).use { fileInStream ->
                IOUtils.copy(fileInStream, archiveOutStream)
            }
            archiveOutStream.closeArchiveEntry()
        }
        return Files.newInputStream(archivePath)
    } catch (e: Exception) {
        Files.delete(archivePath)
        throw e
    }
}

/**
 * Copy output files from the given docker container to the local file system
 *
 * @param dockerClient: DockerClient used to push the files
 * @param runBasePath: Local file system path for the current run. Output files will be copied here.
 * @param dockerDataDir: Directory the output files will be copied from in the docker container
 * @param containerId: ID for the container the files will be copied from.
 * @param outputFiles: Output files as WFiles, which contain partial paths only.
 */
private fun copyOutputFiles(dockerClient: DockerClient, runBasePath: Path, dockerDataDir: String, containerId: String,
                            outputFiles: Set<WFile>) {
    if (outputFiles.isEmpty()) return
    Files.createDirectories(runBasePath)
    outputFiles.forEach { outputFile ->
        val tarStream = dockerClient.copyArchiveFromContainerCmd(containerId, "$dockerDataDir/${outputFile.path}").exec()
        extractTarStream(
            tarStream,
            runBasePath.resolve(OUTPUTS_DIR).resolve(outputFile.path).parent
        )
    }
}

/**
 * Extracts a tar archive InputStream to files in the given output directory
 *
 * @param tarInputStream: The tar archive InputStream
 * @param outBasePath: The directory the files will be extracted to.
 */
private fun extractTarStream(tarInputStream: InputStream, outBasePath: Path) {
    Files.createDirectories(outBasePath)
    TarArchiveInputStream(tarInputStream).use { tarStream ->
        while (true) {
            val tarEntry = tarStream.nextEntry?: break
            val tarFileName = outBasePath.resolve(tarEntry.name)
            if (tarEntry.isDirectory) {
                Files.createDirectories(tarFileName)
                continue
            }

            BufferedOutputStream(FileOutputStream(tarFileName.toString())).use { outStream ->
                val size = tarEntry.size.toInt()
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                var remaining = size
                while (remaining > 0) {
                    val len = tarInputStream.read(buffer, 0, Math.min(remaining, buffer.size))
                    outStream.write(buffer, 0, len)
                    remaining -= len
                }
            }
        }
    }
}