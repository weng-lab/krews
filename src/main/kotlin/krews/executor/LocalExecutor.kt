package krews.executor

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerCertificates
import com.spotify.docker.client.DockerClient
import com.spotify.docker.client.messages.ContainerConfig
import krews.File
import krews.config.DockerConfig
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.db.TaskRun
import krews.db.WorkflowRun
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
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

    override fun copyCachedOutputs(workflowRun: WorkflowRun, cachedOutputTask: TaskRun) {
        val runBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, workflowRun.startTime.millis.toString())
        val previousRunBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, cachedOutputTask.workflowRun.startTime.millis.toString())
        val outputFiles = getFilesForObject(cachedOutputTask)
        outputFiles.forEach {
            Files.copy(previousRunBasePath.resolve(it.path), runBasePath.resolve(it.path))
        }
    }

    override fun executeTask(workflowRun: WorkflowRun, taskConfig: TaskConfig, image: String, script: String?, inputItem: Any, outputItem: Any?) {
        val runBasePath = Paths.get(workflowBasePath.toString(), RUN_DIR, workflowRun.startTime.millis.toString())

        // Create container configuration
        val containerConfig = ContainerConfig.builder().image(image)
        if (script != null) {
            containerConfig.entrypoint("/bin/sh -c")
            containerConfig.cmd(script)
        }

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
    outputFiles.forEach { outputFile ->
        TarArchiveInputStream(dockerClient.archiveContainer(containerId, outputFile.path)).use { tarStream ->
            while (true) {
                val tarEntry = tarStream.nextTarEntry?: break
                val outputPath = runBasePath.resolve(outputFile.path)
                tarEntry.file?.copyTo(outputPath.toFile())
            }
        }
    }
}