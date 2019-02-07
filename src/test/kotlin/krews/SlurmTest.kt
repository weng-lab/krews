package krews

import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.specs.StringSpec
import krews.misc.CommandExecutor
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

class SlurmExecutorTests : StringSpec() {
    override fun tags() = setOf(E2E)

    override fun beforeSpec(description: Description, spec: Spec) {
        //val commandExecutor = CommandExecutor()
    }

    init {
        "test" {


            val sshProcess = Runtime.getRuntime().exec("ssh -tt brooksj@localhost -p 12222")
            //val sshProcess = ProcessBuilder("ssh", "-tt", "brooksj@localhost", "-p", "12222").inheritIO().start()
            val writer = sshProcess.outputStream.writer().buffered()
            writer.write("exit\n")
            writer.flush()
            log.info { sshProcess.inputStream.reader().readLines() }

        }
    }
}