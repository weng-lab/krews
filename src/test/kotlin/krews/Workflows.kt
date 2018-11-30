package krews

import krews.core.CacheIgnored
import krews.core.workflow
import krews.file.*
import reactor.core.publisher.toFlux
import java.nio.file.Files
import java.nio.file.Paths

private data class LocalWorkflowParams(
    val sampleFilesDir: String
)

interface TestBaseInputType { val file: File }
data class TestComplexInputType (
    override val file: File,
    @CacheIgnored
    val cacheIgnoredVal: Double
) : TestBaseInputType

val localFilesWorkflow = workflow("local-files-workflow") {
    val params = params<LocalWorkflowParams>()
    val sampleFiles = Files.newDirectoryStream(Paths.get(params.sampleFilesDir)).sortedBy { f -> f.fileName }
        .map { TestComplexInputType(LocalInputFile(it.toAbsolutePath().toString(), it.fileName.toString()), Math.random()) }
        .toFlux()

    val base64 = task<TestBaseInputType, File>("base64") {
        dockerImage = "alpine:3.8"

        input = sampleFiles
        outputFn { OutputFile("base64/${inputEl.file.filenameNoExt()}.b64") }
        commandFn {
            """
            mkdir -p /data/base64
            base64 /data/${inputEl.file.path} > /data/base64/${inputEl.file.filenameNoExt()}.b64
            """
        }
    }

    task<File, File>("gzip") {
        dockerImage = "alpine:3.8"

        input = base64.output
        outputFn { OutputFile("gzip/${inputEl.filename()}.gz") }
        commandFn {
            """
            mkdir -p /data/gzip
            gzip /data/${inputEl.path} > /data/gzip/${inputEl.filename()}.gz
            """
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

    val base64 = task<File, File>("base64") {
        dockerImage = "alpine:3.8"

        input = inputFiles
        outputFn { OutputFile("base64/${inputEl.filenameNoExt()}.b64") }
        commandFn {
            """
            mkdir -p /data/base64
            base64 /data/${inputEl.path} > /data/base64/${inputEl.filenameNoExt()}.b64
            """
        }
    }

    task<File, File>("gzip") {
        dockerImage = "alpine:3.8"

        input = base64.output
        outputFn { OutputFile("gzip/${inputEl.filename()}.gz") }
        commandFn {
            """
            mkdir -p /data/gzip
            gzip /data/${inputEl.path} > /data/gzip/${inputEl.filename()}.gz
            """
        }
    }
}