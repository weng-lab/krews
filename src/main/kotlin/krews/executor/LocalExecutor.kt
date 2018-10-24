package krews.executor

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerCertificates
import com.spotify.docker.client.DockerClient
import com.spotify.docker.client.messages.ContainerConfig
import krews.File
import krews.config.DockerConfig
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


val DEFAULT_LOCAL_BASE_DIR = "workflow-out"
val RUN_DIR = "run"
val DB_FILENAME = "metadata.db"

class LocalExecutor(workflowConfig: WorkflowConfig) : EnvironmentExecutor {

    private val dockerClient = buildDockerClient(workflowConfig.localExec?.docker?: DockerConfig())
    private val workflowBasePath = Paths.get(workflowConfig.localExec?.localBaseDir ?: DEFAULT_LOCAL_BASE_DIR).toAbsolutePath()!!

    override fun prepareDatabaseFile(): String {
        Files.createDirectories(workflowBasePath)
        return workflowBasePath.resolve(DB_FILENAME).toString()
    }

    override fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<File>) {
        val fromBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, fromWorkflowDir)
        val toBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, toWorkflowDir)
        outputFiles.forEach {
            Files.copy(fromBasePath.resolve(it.path), toBasePath.resolve(it.path))
        }
    }

    override fun executeTask(workflowRunDir: String, taskConfig: TaskConfig, image: String, script: String?, inputItem: Any, outputItem: Any?) {
        val runBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, workflowRunDir)

        // Create container configuration
        val containerConfig = ContainerConfig.builder().image(image)
        if (script != null) {
            containerConfig.entrypoint("/bin/sh", "-c")
            containerConfig.cmd(script)
        }

        // Pull image from remote
        dockerClient.pull(image)

        // Create the docker container from config
        val containerCreation = dockerClient.createContainer(containerConfig.build())
        val containerId = containerCreation.id()!!

        // Copy input files into the docker container
        copyInputFiles(dockerClient, runBasePath, containerId, getFilesForObject(inputItem))

        // Start the container and wait for it to finish processing
        dockerClient.startContainer(containerId)
        dockerClient.waitContainer(containerId)

        // Copy output files out of docker container
        if (outputItem != null) {
            copyOutputFiles(dockerClient, runBasePath, containerId, getFilesForObject(outputItem))
        }
    }

}

private fun buildDockerClient(config: DockerConfig): DockerClient {
    val builder = DefaultDockerClient.fromEnv()
    if (config.uri != null) builder.uri(config.uri)
    if (config.certificatesPath != null) builder.dockerCertificates(DockerCertificates(Paths.get(config.certificatesPath)))
    builder.readTimeoutMillis(config.readTimeout)
    builder.connectTimeoutMillis(config.connectTimeout)
    builder.connectionPoolSize(config.connectionPoolSize)
    return builder.build()
}

private fun copyInputFiles(dockerClient: DockerClient, runBasePath: Path, containerId: String, inputFiles: Set<File>) {
    if (inputFiles.isEmpty()) return
    inputFiles.forEach { inputFile ->
        dockerClient.copyToContainer(runBasePath.resolve(inputFile.path), containerId, inputFile.path)
    }
}

private fun copyOutputFiles(dockerClient: DockerClient, runBasePath: Path, containerId: String, outputFiles: Set<File>) {
    if (outputFiles.isEmpty()) return
    Files.createDirectories(runBasePath)
    outputFiles.forEach { outputFile ->
        extractTarStream(dockerClient.archiveContainer(containerId, outputFile.path), runBasePath.resolve(outputFile.path).parent)
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