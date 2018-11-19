package krews

import krews.core.workflow
import krews.file.GSInputFile
import krews.file.InputFile
import krews.file.LocalInputFile
import krews.file.OutputFile
import reactor.core.publisher.toFlux
import java.nio.file.Files
import java.nio.file.Paths

val localFilesWorkflow = workflow("local-files-workflow") {
    val sampleFilesDir = params.get<String>("sample-files-dir")
    val sampleFiles = Files.newDirectoryStream(Paths.get(sampleFilesDir)).sortedBy { f -> f.fileName }
        .map { LocalInputFile(it.toAbsolutePath().toString(), it.fileName.toString()) }
        .toFlux()

    val base64 = task<InputFile, OutputFile>("base64") {
        dockerImage = "alpine:3.8"

        input = sampleFiles
        outputFn { OutputFile("base64/${inputItem.filenameNoExt()}.b64") }
        commandFn {
            """
            mkdir -p /data/base64
            base64 /data/${inputItem.path} > /data/base64/${inputItem.filenameNoExt()}.b64
            """.trimIndent()
        }
    }

    task<OutputFile, OutputFile>("gzip") {
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

val gsFilesWorkflow = workflow("gs-files-workflow") {
    val inputBucket = params.get<String>("input-files-bucket")
    val inputBaseDir = params.get<String>("input-files-base-dir")
    val inputFiles = params.get<List<String>>("input-files")
        .map { GSInputFile(inputBucket, "$inputBaseDir/$it", it) }
        .toFlux()

    val base64 = task<InputFile, OutputFile>("base64") {
        dockerImage = "alpine:3.8"

        input = inputFiles
        outputFn { OutputFile("base64/${inputItem.filenameNoExt()}.b64") }
        commandFn {
            """
            mkdir -p /data/base64
            base64 /data/${inputItem.path} > /data/base64/${inputItem.filenameNoExt()}.b64
            """.trimIndent()
        }
    }

    task<OutputFile, OutputFile>("gzip") {
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