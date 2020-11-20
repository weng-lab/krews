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

data class Base64TaskParams(val someVal: String, val someFiles: List<File>?)

data class EchoTaskParams(val value: String)

data class LocalGzipOutput(val gzip: File, val missingOptional: File)

data class Base64TaskOutput(val base64: File, val extra: OutputDirectory)

data class EchoTaskOutput(val output: File)

fun localFilesWorkflow() = workflow("local-files-workflow") {
    val params = params<LocalWorkflowParams>()
    val sampleFiles = Files.newDirectoryStream(Paths.get(params.sampleFilesDir)).sortedBy { f -> f.fileName }
        .map {
            TestComplexInputType(LocalInputFile(it.toAbsolutePath().toString(), it.fileName.toString()))
        }
        .toFlux()

    val base64 = task<TestBaseInputType, Base64TaskOutput>("base64", sampleFiles) {
        val taskParams = taskParams<Base64TaskParams>()
        val file = input.file
        dockerImage = "alpine:3.8"
        output = Base64TaskOutput(OutputFile("base64/${file.filenameNoExt()}.b64"), OutputDirectory("extra"))
        command =
            """
            echo ${taskParams.someVal}
            mkdir -p $(dirname ${output!!.base64.dockerPath})
            base64 ${file.dockerPath} > ${output!!.base64.dockerPath}
            mkdir -p ${output!!.extra.dockerPath}
            echo "one" > ${output!!.extra.dockerPath}/one.txt
            echo "two" > ${output!!.extra.dockerPath}/two.txt
            """
    }

    task<Any, EchoTaskOutput>("echo", listOf(0).toFlux()) {
        val taskParams = taskParams<EchoTaskParams>()
        dockerImage = "krewstest"
        output = EchoTaskOutput(OutputFile("echo/output.txt"))
        command =
            """
            mkdir -p $(dirname ${output!!.output.dockerPath})
            /test.sh ${taskParams.value} ${output!!.output.dockerPath}
            """
    }

    task<File, LocalGzipOutput>("gzip", base64.map { it.base64 }) {
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

    task<File, OutputFile>("gzip-extra", base64.flatMap { it.extra.files.flatMapMany { it.toFlux() } }) {
        dockerImage = "alpine:3.8"
        output = OutputFile("gzip/${input.filename()}.gz")
        command =
            """
            echo running gzip on ${input.path}
            mkdir -p $(dirname ${output!!.dockerPath})
            gzip -c ${input.dockerPath} > ${output!!.dockerPath}
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

    val base64 = task<File, Base64TaskOutput>("base64", inputFiles) {
        dockerImage = "alpine:3.8"
        output = Base64TaskOutput(OutputFile("base64/${input.filenameNoExt()}.b64"), OutputDirectory("extra"))
        command =
            """
            mkdir -p $(dirname ${output!!.base64.dockerPath})
            base64 ${input.dockerPath} > ${output!!.base64.dockerPath}
            mkdir -p ${output!!.extra.dockerPath}
            echo "one" > ${output!!.extra.dockerPath}/one.txt
            echo "two" > ${output!!.extra.dockerPath}/two.txt
            """
    }

    task<File, GSGzipOutput>("gzip", base64.map { it.base64 }) {
        dockerImage = "alpine:3.8"
        val outGz = OutputFile("gzip/${input.filename()}.gz")
        output = GSGzipOutput(outGz, OutputFile("gzip/${input.filename()}.fake", optional = true))
        command =
            """
            mkdir -p $(dirname ${outGz.dockerPath})
            gzip ${input.dockerPath} > ${outGz.dockerPath}
            """
    }

    task<File, OutputFile>("gzip-extra", base64.flatMap { it.extra.files.flatMapMany { it.toFlux() } }) {
        dockerImage = "alpine:3.8"
        output = OutputFile("gzip/${input.filename()}.gz")
        command =
            """
            mkdir -p $(dirname ${output!!.dockerPath})
            gzip -c ${input.dockerPath} > ${output!!.dockerPath}
            """
    }
}