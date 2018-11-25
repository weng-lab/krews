package krews

import krews.core.workflow
import krews.file.GSInputFile
import krews.file.InputFile
import krews.file.LocalInputFile
import krews.file.OutputFile
import reactor.core.publisher.toFlux
import java.nio.file.Files
import java.nio.file.Paths

private data class LocalWorkflowParams(
    val sampleFilesDir: String
)

val localFilesWorkflow = workflow("local-files-workflow") {
    val params = params<LocalWorkflowParams>()
    val sampleFiles = Files.newDirectoryStream(Paths.get(params.sampleFilesDir)).sortedBy { f -> f.fileName }
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

private data class GSWorkflowParams(
    val inputFilesBucket: String,
    val inputFilesBaseDir: String,
    val inputFiles: List<String>
)

val gsFilesWorkflow = workflow("gs-files-workflow") {
    val params = params<GSWorkflowParams>()
    val inputFiles = params.inputFiles
        .map { GSInputFile(params.inputFilesBucket, "${params.inputFilesBaseDir}/$it", it) }
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