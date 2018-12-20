package krews

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
    override val file: File
) : TestBaseInputType

data class Bast64TaskParams(val someVal: String, val someFiles: List<File>?)

fun localFilesWorkflow() = workflow("local-files-workflow") {
    val params = params<LocalWorkflowParams>()
    val sampleFiles = Files.newDirectoryStream(Paths.get(params.sampleFilesDir)).sortedBy { f -> f.fileName }
        .map { TestComplexInputType(LocalInputFile(it.toAbsolutePath().toString(), it.fileName.toString(), true)) }
        .toFlux()

    val base64 = task<TestBaseInputType, File>("base64", sampleFiles) {
        val taskParams = taskParams<Bast64TaskParams>()
        val file = input.file
        dockerImage = "alpine:3.8"
        output = OutputFile("base64/${file.filenameNoExt()}.b64")
        command =
            """
            echo ${taskParams.someVal}
            mkdir -p /data/base64
            base64 /data/${file.path} > /data/base64/${file.filenameNoExt()}.b64
            """
    }

    task<File, File>("gzip", base64.outputPub) {
        dockerImage = "alpine:3.8"
        output = OutputFile("gzip/${input.filename()}.gz")
        command =
            """
            echo running gzip on ${input.path}
            mkdir -p /data/gzip
            gzip /data/${input.path} > /data/gzip/${input.filename()}.gz
            """
    }
}

private data class GSWorkflowParams(
    val inputFilesBucket: String,
    val inputFilesBaseDir: String,
    val inputFiles: List<String>,
    val cacheInputFiles: Boolean
)

fun gsFilesWorkflow() = workflow("gs-files-workflow") {
    val params = params<GSWorkflowParams>()
    val inputFiles = params.inputFiles
        .map { GSInputFile(params.inputFilesBucket, "${params.inputFilesBaseDir}/$it", it, cache = params.cacheInputFiles) }
        .toFlux()

    val base64 = task<File, File>("base64", inputFiles) {
        dockerImage = "alpine:3.8"
        output = OutputFile("base64/${input.filenameNoExt()}.b64")
        command =
            """
            mkdir -p /data/base64
            base64 /data/${input.path} > /data/base64/${input.filenameNoExt()}.b64
            """
    }

    task<File, File>("gzip", base64.outputPub) {
        dockerImage = "alpine:3.8"
        output = OutputFile("gzip/${input.filename()}.gz")
        command =
            """
            mkdir -p /data/gzip
            gzip /data/${input.path} > /data/gzip/${input.filename()}.gz
            """
    }
}