package krews.executor

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.PullImageResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import krews.WFile
import krews.config.DockerConfig
import krews.config.TaskConfig
import krews.config.WorkflowConfig
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


val DEFAULT_LOCAL_BASE_DIR = "workflow-out"

private val log = KotlinLogging.logger {}

class LocalExecutor(workflowConfig: WorkflowConfig) : EnvironmentExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.localExec?.docker?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.localExec?.localBaseDir ?: DEFAULT_LOCAL_BASE_DIR).toAbsolutePath()!!

    override fun prepareDatabaseFile(): String {
        Files.createDirectories(workflowBasePath)
        return workflowBasePath.resolve(DB_FILENAME).toString()
    }

    override fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<WFile>) {
        val fromBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, fromWorkflowDir)
        val toBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, toWorkflowDir)
        outputFiles.forEach {
            val fromPath = fromBasePath.resolve(it.path)
            val toPath = toBasePath.resolve(it.path)
            log.info { "Copying cached file from $fromPath to $toPath" }
            Files.createDirectories(toPath.parent)
            Files.copy(fromPath, toPath)
        }
    }

    override fun executeTask(workflowRunDir: String, taskConfig: TaskConfig, image: String, script: String?, inputItem: Any, outputItem: Any?) {
        val runBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, workflowRunDir)

        // Pull image from remote
        log.info { "Pulling image \"$image\" from remote..." }
        dockerClient.pullImageCmd(image).exec(PullImageResultCallback()).awaitSuccess()

        // Create the docker container from config
        log.info { "Creating container from image \"$image\"" }
        val containerCreationCmd = dockerClient.createContainerCmd(image)
        if (script != null) {
            containerCreationCmd.withEntrypoint("/bin/sh", "-c")
            containerCreationCmd.withCmd(script)
        }
        val createContainerResponse = containerCreationCmd.exec()
        val containerId = createContainerResponse.id!!

        // Copy input files into the docker container
        val inputFiles = getFilesForObject(inputItem)
        if (!inputFiles.isEmpty()) {
            log.info { "Copying input files $inputFiles into newly created container $containerId" }
            copyInputFiles(dockerClient, runBasePath, containerId, inputFiles)
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
            copyOutputFiles(dockerClient, runBasePath, containerId, outputFiles)
        }
    }

}

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

private fun copyInputFiles(dockerClient: DockerClient, runBasePath: Path, containerId: String, inputFiles: Set<WFile>) {
    if (inputFiles.isEmpty()) return
    inputFiles.forEach { inputFile ->
        val tarInputStream = createTarStream(runBasePath, Paths.get(inputFile.path))
        dockerClient.copyArchiveToContainerCmd(containerId)
            .withTarInputStream(tarInputStream)
            //.withRemotePath(inputFile.path)
            .exec()
    }
}

private fun createTarStream(basePath: Path, filePath: Path): InputStream {
    val archivePath = Files.createTempFile("docker-client-", ".tar.gz")
    try {
        TarArchiveOutputStream(GZIPOutputStream(BufferedOutputStream(Files.newOutputStream(archivePath)))).use { archiveOutStream ->
            val fullFilePath = basePath.resolve(filePath)
            val tarEntry = TarArchiveEntry(filePath.toString())
            tarEntry.size = Files.size(fullFilePath)
            archiveOutStream.putArchiveEntry(tarEntry)
            BufferedInputStream(Files.newInputStream(fullFilePath)).use { fileInStream ->
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

private fun copyOutputFiles(dockerClient: DockerClient, runBasePath: Path, containerId: String, outputFiles: Set<WFile>) {
    if (outputFiles.isEmpty()) return
    Files.createDirectories(runBasePath)
    outputFiles.forEach { outputFile ->
        val tarStream = dockerClient.copyArchiveFromContainerCmd(containerId, outputFile.path.toString()).exec()
        extractTarStream(tarStream, runBasePath.resolve(outputFile.path).parent)
    }
}

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