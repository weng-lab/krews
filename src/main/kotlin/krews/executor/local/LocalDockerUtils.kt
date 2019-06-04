package krews.executor.local

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.*
import com.github.dockerjava.core.*
import com.github.dockerjava.core.command.WaitContainerResultCallback
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import krews.config.DockerConfig
import krews.file.*
import java.nio.file.*


/**
 * Build a DockerClient from a given DockerConfig
 */
internal fun buildDockerClient(config: DockerConfig): DockerClient {
    val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder()
    if (config.uri != null) { configBuilder.withDockerHost(config.uri) }
    if (config.certificatesPath != null) { configBuilder.withDockerCertPath(config.certificatesPath) }
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
 * Downloads the given input file with a new container. The file will be available in the mounted directory
 */
internal fun downloadRemoteInputFile(dockerClient: DockerClient, inputFile: InputFile, dockerDataDir: String, mountDir: Path) {
    if (inputFile is LocalInputFile) {
        return
    }
    val volume = Volume(dockerDataDir)
    val downloadContainerId = dockerClient.createContainerCmd(inputFile.downloadFileImage())
        .withVolumes(volume)
        .withBinds(Bind(mountDir.toString(), volume))
        .withCmd(inputFile.downloadFileCommand(dockerDataDir))
        .exec().id!!
    dockerClient.startContainerCmd(downloadContainerId).exec()
    dockerClient.waitContainerCmd(downloadContainerId).exec(WaitContainerResultCallback()).awaitCompletion()
    dockerClient.removeContainerCmd(downloadContainerId).exec()
}