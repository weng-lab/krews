package krews.misc

import krews.config.SshConfig
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Runs arbitrary shell commands, optionally via ssh.
 */
class CommandExecutor(private val sshConfig: SshConfig? = null) {

    fun exec(command: String): String {
        log.info { "Executing command:\n$command" }

        val runCmd =
            if (sshConfig != null)
                arrayOf("ssh", "${sshConfig.user}@${sshConfig.host}", "-p", "${sshConfig.port}", "-X", command)
            else
                arrayOf("/bin/sh", "-c", command)
        val process = Runtime.getRuntime().exec(runCmd)
        val result = process.inputStream.reader().readText()
        log.info { "Command results:\n$result" }

        val error = process.errorStream.reader().readText()
        val exitCode = process.waitFor()
        if (exitCode != 0) throw Exception("Encountered error during command execution: $error")
        return result
    }

}
