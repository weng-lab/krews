package krews.misc

import krews.config.SshConfig
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Runs arbitrary shell commands, optionally via ssh.
 */
class CommandExecutor(private val sshConfig: SshConfig?) {

    fun exec(command: String): String {
        log.debug { "Executing command:\n$command" }

        val runCmd = if (sshConfig != null) "ssh ${sshConfig.user}@${sshConfig.host} -p ${sshConfig.port} -X $command"
            else command
        val process = Runtime.getRuntime().exec(runCmd)
        val result = process.inputStream.reader().readText()
        log.debug { "Command results:\n$result" }

        val error = process.errorStream.reader().readText()
        if (error.isNotBlank()) throw Exception("Encountered error during command execution: $error")
        return result
    }

}