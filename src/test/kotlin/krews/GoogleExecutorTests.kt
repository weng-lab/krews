package krews

import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.Bucket
import com.google.api.services.storage.model.StorageObject
import com.typesafe.config.ConfigFactory
import io.kotlintest.*
import io.kotlintest.specs.StringSpec
import io.mockk.spyk
import krews.config.createParamsForConfig
import krews.config.createWorkflowConfig
import krews.core.Params
import krews.core.WorkflowRunner
import krews.executor.google.GoogleLocalExecutor
import krews.executor.google.googleStorageClient
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.*


const val googleProjectId = "devenv-215523"
const val inputFilesDir = "test-input-files"
const val workflowBaseDir = "workflow-test"
val testBucket = "krews-test-${UUID.randomUUID()}"
fun googleConfig(inputFiles: List<String>) =
    """
    google {
        storage-bucket = "$testBucket"
        storage-base-dir = "$workflowBaseDir"
        local-storage-base-dir = "google-workflow-test"
        project-id = "$googleProjectId"
        regions = ["us-east1", "us-east4"]
        job-completion-poll-interval = 5
        log-upload-interval = 10
    }
    params {
        input-files-bucket = "$testBucket"
        input-files-base-dir = "$inputFilesDir"
        input-files = ${inputFiles.joinToString(",", "[", "]", -1, "...") {"\"$it\""}}
    }
    """.trimIndent()

class GoogleExecutorTests : StringSpec() {
    override fun tags() = setOf(E2E)

    override fun beforeSpec(description: Description, spec: Spec) {
        // Create a bucket to use for this set of tests
        val bucket = Bucket()
        bucket.name = testBucket
        googleStorageClient.buckets().insert(googleProjectId, bucket).execute()
    }

    override fun afterSpec(description: Description, spec: Spec) {
        // Delete test bucket
        //googleStorageClient.buckets().delete(testBucket)
    }

    init {
        "Can run a simple workflow on Google Cloud Pipelines API" {
            // Create 2 test files in our google storage bucket
            for (i in 1..2) {
                writeFileToBucket(testBucket, "$inputFilesDir/test-$i.txt", "I am test file $i")
            }

            runWorkflow((1..2).map { "test-$it.txt" }, 1)

            "state/metadata.db" should existInGS(testBucket, workflowBaseDir)
            val runDir = "run/1"
            for (i in 1..2) {
                "$runDir/inputs/test-$i.txt" should existInGS(testBucket, workflowBaseDir)
                "$runDir/outputs/base64/test-$i.b64" should existInGS(testBucket, workflowBaseDir)
                "$runDir/outputs/gzip/test-$i.b64.gz" should existInGS(testBucket, workflowBaseDir)
            }
        }

        "Can do a second run of the same simple workflow on google cloud with cached inputs and outputs" {
            // Overwrite file 1
            writeFileToBucket(testBucket, "$inputFilesDir/test-1.txt", "I am an updated file")

            // Create a new file (3)
            writeFileToBucket(testBucket, "$inputFilesDir/test-3.txt", "I am test file 3")

            val executor = runWorkflow((1..3).map { "test-$it.txt" }, 2)

            val runDir = "run/2"
            for (i in 1..3) {
                "$runDir/inputs/test-$i.txt" should existInGS(testBucket, workflowBaseDir)
                "$runDir/outputs/base64/test-$i.b64" should existInGS(testBucket, workflowBaseDir)
                "$runDir/outputs/gzip/test-$i.b64.gz" should existInGS(testBucket, workflowBaseDir)
            }

            verifyDownloadInputFile(executor, "test-1.txt")
            verifyCachedInputFile(executor, "test-2.txt")
            verifyDownloadInputFile(executor, "test-3.txt")

            // Verify tasks were re-run for test-1 and test-4 and NOT for test-2
            verifyExecuteWithOutput(executor, "base64/test-1.b64")
            verifyExecuteWithOutput(executor, "base64/test-2.b64", 0)
            verifyExecuteWithOutput(executor, "base64/test-3.b64")

            verifyExecuteWithOutput(executor, "gzip/test-1.b64.gz")
            verifyExecuteWithOutput(executor, "gzip/test-2.b64.gz", 0)
            verifyExecuteWithOutput(executor, "gzip/test-3.b64.gz")
        }
    }

    /**
     * Convenience function that runs the SimpleWorkflow and returns a GoogleLocalExecutor spy
     */
    private fun runWorkflow(inputFiles: List<String>, runTimestampOverride: Long): GoogleLocalExecutor {
        val config = ConfigFactory.parseString(googleConfig(inputFiles))
        val workflow = gsFilesWorkflow.build(Params(createParamsForConfig(config)))
        val workflowConfig = createWorkflowConfig(config, workflow)
        val executor = spyk(GoogleLocalExecutor(workflowConfig))
        val runner = WorkflowRunner(workflow, workflowConfig, executor, runTimestampOverride)
        runner.run()
        return executor
    }
}

fun writeFileToBucket(bucket: String, filename: String, content: String) {
    val fileMetadata = StorageObject().setName(filename)
    val contentStream = InputStreamContent("text/plain", ByteArrayInputStream(content.toByteArray()))
    googleStorageClient.objects().insert(bucket, fileMetadata, contentStream).execute()
}

fun existInGS(bucket: String, baseDir: String) = object : Matcher<String> {
    override fun test(value: String): Result {
        var passed = false
        try {
            googleStorageClient.objects().get(bucket, "$baseDir/$value").execute()
            passed = true
        } catch (e: IOException) {}
        return Result(passed,
            "Object $value should exist in google storage in bucket $bucket under $baseDir",
            "Object $value should not exist in google storage in bucket $bucket under $baseDir")
    }
}