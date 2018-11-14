package krews

import krews.core.Workflow
import krews.file.InputFile
import krews.file.LocalInputFile
import krews.file.OutputFile
import reactor.core.publisher.Flux
import java.nio.file.Files
import java.nio.file.Paths

class SimpleWorkflow : Workflow("config-sample") {
    val files = params<String>("sample-files-dir")
        .map { Files.newDirectoryStream(Paths.get(it)).sortedBy { f -> f.fileName } }
        .flatMapMany { Flux.fromIterable(it) }
        .map { LocalInputFile(it.toAbsolutePath().toString(), it.fileName.toString()) }

    val base64 = task<InputFile, OutputFile>("base64") {
        dockerImage = "alpine:3.8"

        input = files
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