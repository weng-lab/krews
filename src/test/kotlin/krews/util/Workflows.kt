package krews.util

import krews.core.workflow
import krews.file.*
import reactor.core.publisher.toFlux
import java.nio.file.*

private data class LocalWorkflowParams(
    val sampleFilesDir: String
)

interface TestBaseInputType { val file: File }
data class TestComplexInputType (
    override val file: File
) : TestBaseInputType

data class Bast64TaskParams(val someVal: String, val someFiles: List<File>?)

data class LocalGzipOutput(val gzip: File, val missingOptional: File)

fun localFilesWorkflow() = workflow("local-files-workflow") {
    val params = params<LocalWorkflowParams>()
    val sampleFiles = Files.newDirectoryStream(Paths.get(params.sampleFilesDir)).sortedBy { f -> f.fileName }
        .map {
            TestComplexInputType(LocalInputFile(it.toAbsolutePath().toString(), it.fileName.toString()))
        }
        .toFlux()

    val base64 = task<TestBaseInputType, File>("base64", sampleFiles) {
        val taskParams = taskParams<Bast64TaskParams>()
        val file = input.file
        dockerImage = "alpine:3.8"
        output = OutputFile("base64/${file.filenameNoExt()}.b64")
        command =
            """
            echo ${taskParams.someVal}
            mkdir -p $(dirname ${output!!.dockerPath})
            base64 ${file.dockerPath} > ${output!!.dockerPath}
            """
    }

    task<File, LocalGzipOutput>("gzip", base64) {
        dockerImage = "alpine:3.8"
        val outGz = OutputFile("gzip/${input.filename()}.gz")
        output = LocalGzipOutput(outGz, OutputFile("gzip/${input.filename()}.fake", optional = true))
        command =
            """
            echo running gzip on ${input.path}
            mkdir -p $(dirname ${outGz.dockerPath})
            gzip -c ${input.dockerPath} > ${outGz.dockerPath}
            """
    }
}

private data class GSWorkflowParams(
    val inputFilesBucket: String,
    val inputFilesBaseDir: String,
    val inputFiles: List<String>
)

data class GSGzipOutput(val gzip: File, val missingOptional: File)

fun gsFilesWorkflow() = workflow("gs-files-workflow") {
    val params = params<GSWorkflowParams>()
    val inputFiles = params.inputFiles
        .map { GSInputFile(params.inputFilesBucket, "${params.inputFilesBaseDir}/$it", it) }
        .toFlux()

    val base64 = task<File, File>("base64", inputFiles) {
        dockerImage = "alpine:3.8"
        output = OutputFile("base64/${input.filenameNoExt()}.b64")
        command =
            """
            mkdir -p $(dirname ${output!!.dockerPath})
            base64 ${input.dockerPath} > ${output!!.dockerPath}
            """
    }

    task<File, GSGzipOutput>("gzip", base64) {
        dockerImage = "alpine:3.8"
        val outGz = OutputFile("gzip/${input.filename()}.gz")
        output = GSGzipOutput(outGz, OutputFile("gzip/${input.filename()}.fake", optional = true))
        command =
            """
            mkdir -p $(dirname ${outGz.dockerPath})
            gzip ${input.dockerPath} > ${outGz.dockerPath}
            """
    }
}