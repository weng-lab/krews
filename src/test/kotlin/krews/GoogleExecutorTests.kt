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
import krews.core.WorkflowRunner
import krews.executor.REPORT_FILENAME
import krews.executor.google.GoogleLocalExecutor
import krews.executor.google.gcsObjectPath
import krews.executor.google.googleStorageClient
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*


class GoogleExecutorTests : StringSpec() {
    override fun tags() = setOf(E2E)

    private val googleProjectId = "devenv-215523"
    private val localFilesDir = "google-workflow-test"
    private val inputFilesDir = "test-input-files"
    private val workflowBaseDir = "workflow-test"
    private val testBucket = "krews-test-${UUID.randomUUID()}"
    private fun googleConfig(inputFiles: List<String>, cacheInputFiles: Boolean) =
        """
        local-files-base-dir = $localFilesDir
        clean-old-files = true
        google {
            storage-bucket = "$testBucket"
            storage-base-dir = "$workflowBaseDir"
            project-id = "$googleProjectId"
            regions = ["us-east1", "us-east4"]
            job-completion-poll-interval = 5
            log-upload-interval = 10
        }
        params {
            input-files-bucket = "$testBucket"
            input-files-base-dir = "$inputFilesDir"
            input-files = ${inputFiles.joinToString(",", "[", "]", -1, "...") {"\"$it\""}}
            cache-input-files = $cacheInputFiles
        }
        """.trimIndent()

    override fun beforeSpec(description: Description, spec: Spec) {
        // Create a bucket to use for this set of tests
        val bucket = Bucket()
        bucket.name = testBucket
        googleStorageClient.buckets().insert(googleProjectId, bucket).execute()
    }

    override fun afterSpec(description: Description, spec: Spec) {
        // Delete test bucket
        val bucketContents = googleStorageClient.objects().list(testBucket).execute()
        bucketContents.items.forEach { googleStorageClient.objects().delete(testBucket, it.name).execute() }
        googleStorageClient.buckets().delete(testBucket).execute()

        // Clean up local temporary dirs
        Files.walk(Paths.get(localFilesDir))
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }

    init {

        "Can run a simple workflow on Google Cloud Pipelines API" {
            // Create 2 test files in our google storage bucket
            for (i in 1..2) {
                writeFileToBucket(testBucket, "$inputFilesDir/test-$i.txt", "I am test file $i")
            }

            val executor = runWorkflow((1..2).map { "test-$it.txt" }, false, 1)

            "state/metadata.db" should existInGS(testBucket, workflowBaseDir)
            for (i in 1..2) {
                verifyInputFileCached(executor, "test-$i.txt", 0)
                "inputs/test-$i.txt" shouldNot existInGS(testBucket, workflowBaseDir)
                "outputs/base64/test-$i.b64" should existInGS(testBucket, workflowBaseDir)
                "outputs/gzip/test-$i.b64.gz" should existInGS(testBucket, workflowBaseDir)
            }

            // Confirm that an html report was generated
            "run/1/$REPORT_FILENAME" should existInGS(testBucket, workflowBaseDir)
        }

        "Can run the same workflow but with input caching on and not re-run tasks unnecessarily" {
            val executor = runWorkflow((1..2).map { "test-$it.txt" }, true, 2)

            // Since inputs are cached now, they are downloaded, but the tasks should not run since the files / inputs have not changed.
            for (i in 1..2) {
                "inputs/test-$i.txt" should existInGS(testBucket, workflowBaseDir)
                verifyInputFileCached(executor, "test-$i.txt")
                verifyExecuteWithOutput(executor, "base64/test-$i.b64", 0)
                verifyExecuteWithOutput(executor, "gzip/test-$i.b64.gz", 0)
            }
        }

        "Can do a second run of the same simple workflow on google cloud with cached inputs and outputs" {
            // Delete file 1
            googleStorageClient.objects().delete(testBucket, "$inputFilesDir/test-1.txt").execute()

            // Overwrite file 2
            writeFileToBucket(testBucket, "$inputFilesDir/test-2.txt", "I have been updated again")

            // Create a new file (3)
            writeFileToBucket(testBucket, "$inputFilesDir/test-3.txt", "I am test file 3")

            val executor = runWorkflow((2..3).map { "test-$it.txt" }, true, 3)

            for (i in 2..3) {
                "inputs/test-$i.txt" should existInGS(testBucket, workflowBaseDir)
                "outputs/base64/test-$i.b64" should existInGS(testBucket, workflowBaseDir)
                "outputs/gzip/test-$i.b64.gz" should existInGS(testBucket, workflowBaseDir)
                verifyInputFileCached(executor, "test-$i.txt")
                verifyExecuteWithOutput(executor, "base64/test-$i.b64")
                verifyExecuteWithOutput(executor, "gzip/test-$i.b64.gz")
            }

            // Make sure file 1 related inputs and outputs were deleted due to clear-old-files config
            "inputs/test-1.txt" shouldNot existInGS(testBucket, workflowBaseDir)
            "outputs/base64/test-1.b64" shouldNot existInGS(testBucket, workflowBaseDir)
            "outputs/gzip/test-1.b64" shouldNot existInGS(testBucket, workflowBaseDir)
        }
    }

    /**
     * Convenience function that runs the SimpleWorkflow and returns a GoogleLocalExecutor spy
     */
    private fun runWorkflow(inputFiles: List<String>, cacheInputFiles: Boolean, runTimestampOverride: Long): GoogleLocalExecutor {
        val config = ConfigFactory.parseString(googleConfig(inputFiles, cacheInputFiles))
        val workflow = gsFilesWorkflow().build(createParamsForConfig(config))
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