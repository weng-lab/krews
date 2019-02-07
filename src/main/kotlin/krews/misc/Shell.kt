package krews.misc

import krews.config.SshConfig
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Runs arbitrary shell commands, optionally via ssh.
 */
class CommandExecutor(sshConfig: SshConfig?) {

    private val sshProcess =
        if (sshConfig != null)
            Runtime.getRuntime().exec("ssh -tt ${sshConfig.user}@${sshConfig.host} -p ${sshConfig.port}")
        else null

    private val sshProcessWriter = sshProcess?.outputStream?.writer()?.buffered()

    fun exec(command: String) {
        log.info { "Executing command:\n$command" }
        val resultsStream = if (sshProcessWriter != null) {
            sshProcessWriter.write("$command\n")
            sshProcessWriter.write("exit\n")
            sshProcessWriter.flush()
            sshProcess!!.inputStream
        } else {
            val process = Runtime.getRuntime().exec(command)
            process.inputStream
        }
        log.info { "Command results:\n${resultsStream.reader().readText()}" }
    }

}