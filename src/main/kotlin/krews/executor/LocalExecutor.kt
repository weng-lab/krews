package krews.executor

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.PullImageResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import krews.TaskDocker
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


const val DEFAULT_LOCAL_BASE_DIR = "workflow-out"

private val log = KotlinLogging.logger {}

class LocalExecutor(workflowConfig: WorkflowConfig) : EnvironmentExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.localExec?.docker?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.localExec?.localBaseDir ?: DEFAULT_LOCAL_BASE_DIR).toAbsolutePath()!!

    override fun prepareDatabaseFile(): String {
        Files.createDirectories(workflowBasePath)
        return workflowBasePath.resolve(DB_FILENAME).toString()
    }

    override fun pushDatabaseFile() {}

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

    override fun executeTask(workflowRunDir: String, taskConfig: TaskConfig, taskDocker: TaskDocker, script: String?,
                             inputItem: Any, outputItem: Any?) {
        val runBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, workflowRunDir)

        // Pull image from remote
        log.info { "Pulling image \"${taskDocker.image}\" from remote..." }
        dockerClient.pullImageCmd(taskDocker.image).exec(PullImageResultCallback()).awaitSuccess()

        // Create the docker container from config
        log.info { "Creating container from image \"${taskDocker.image}\"" }
        val containerCreationCmd = dockerClient.createContainerCmd(taskDocker.image)
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
            copyInputFiles(dockerClient, runBasePath, taskDocker.dataDir, containerId, inputFiles)
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
            copyOutputFiles(dockerClient, runBasePath, taskDocker.dataDir, containerId, outputFiles)
        }
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
        val tarInputStream = createTarStream(runBasePath, Paths.get(inputFile.path))
        dockerClient.copyArchiveToContainerCmd(containerId)
            .withTarInputStream(tarInputStream)
            .withRemotePath("$dockerDataDir/${inputFile.path}")
            .exec()
    }
}

/**
 * Creates a Tar Archive as an InputStream for the given file.
 *
 * @param basePath: Directory the file can be found in. This will not be part of the path in the tar archive.
 * @param filePath: Partial path for the file that will be archived. This is the only part that will be in the archive.
 */
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
        extractTarStream(tarStream, runBasePath.resolve(outputFile.path).parent)
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