package krews

import krews.core.Workflow
import krews.file.InputFile
import krews.file.LocalInputFile
import krews.file.OutputFile
import reactor.core.publisher.toFlux
import java.nio.file.Files

object SimpleWorkflow : Workflow("config-sample") {
    // Create 3 files in a temp directory to use as inputs.
    val tempDir = Files.createTempDirectory("test")!!
    val tempFiles = (1..3)
        .map { Files.createFile(tempDir.resolve("test-$it.txt")) }
        .map { Files.write(it, "I am a test file".toByteArray()) }
        .map { LocalInputFile(it.toString(), it.fileName.toString()) }
        .toFlux()

    val base64 = task<InputFile, OutputFile>("base64") {
        dockerImage = "alpine:3.8"

        input = tempFiles
        outputFn { OutputFile("base64/${inputItem.filenameNoExt()}.b64") }
        commandFn {
            """
            mkdir -p /data/base64
            base64 /data/${inputItem.path} > /data/base64/${inputItem.filenameNoExt()}.b64
            """.trimIndent()
        }
    }

    val gzip = task<OutputFile, OutputFile>("gzip") {
        dockerImage = "alpine:3.8"

        input = base64.output
        outputFn { OutputFile("gzip/${inputItem.filename()}.gz") }
        commandFn {
            """
            mkdir -p /data/gzip
            gzip /data/${inputItem.path} > /data/gzip/${inputItem.filename()}.gz
            """.trimIndent()
        }
    }
}