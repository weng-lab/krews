package krews

import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.*
import com.typesafe.config.ConfigFactory
import io.mockk.spyk
import krews.config.*
import krews.core.*
import krews.executor.google.*
import krews.util.*
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*
import java.io.*
import java.util.*


@Disabled
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class GoogleExecutorTests {

    private val googleProjectId = "devenv-215523"
    private val inputFilesDir = "test-input-files"
    private val workflowBaseDir = "workflow-test"
    private val testBucket = "krews-test-${UUID.randomUUID()}"
    private fun googleConfig(inputFiles: List<String>, grouping: Int) =
        """
        working-dir = $workflowBaseDir
        task.default {
            grouping = $grouping
        }
        google {
            bucket = "$testBucket"
            project-id = "$googleProjectId"
            regions = ["us-east1", "us-east4"]
            job-completion-poll-interval = 5
            log-upload-interval = 10
        }
        params {
            input-files-bucket = "$testBucket"
            input-files-base-dir = "$inputFilesDir"
            input-files = ${inputFiles.joinToString(",", "[", "]") { "\"$it\"" }}
        }
        """.trimIndent()

    @BeforeAll
    fun beforeTest() {
        // Create a bucket to use for this set of tests
        val bucket = Bucket()
        bucket.name = testBucket
        googleStorageClient.buckets().insert(googleProjectId, bucket).execute()
    }

    //@AfterAll
    fun afterTest() {
        // Delete test bucket
        val bucketContents = googleStorageClient.objects().list(testBucket).execute()
        bucketContents.items.forEach { googleStorageClient.objects().delete(testBucket, it.name).execute() }
        googleStorageClient.buckets().delete(testBucket).execute()
    }

    @Test @Order(1)
    fun `Can run a simple workflow on Google Cloud Pipelines API`() {
        // Create 3 test files in our google storage bucket
        for (i in 1..3) {
            writeFileToBucket(testBucket, "$inputFilesDir/test-$i.txt", "I am test file $i")
        }

        runWorkflow((1..3).map { "test-$it.txt" }, grouping = 2, runTimestampOverride = 1)

        for (i in 1..3) {
            "outputs/base64/test-$i.b64".existsInGS(testBucket, workflowBaseDir)
            "outputs/gzip/test-$i.b64.gz".existsInGS(testBucket, workflowBaseDir)
            "outputs/gzip/test-$i.b64.fake.none".existsInGS(testBucket, workflowBaseDir)
        }

        // Confirm that an html report was generated
        "run/1/$REPORT_FILENAME".existsInGS(testBucket, workflowBaseDir)
    }

    @Test @Order(2)
    fun `Can run the same workflow but cache and not re-run tasks unnecessarily`() {
        // Overwrite input file 1
        writeFileToBucket(testBucket, "$inputFilesDir/test-1.txt", "I have been updated")

        // Delete base64 output file 2 so we rerun tasks for file 2
        googleStorageClient.objects().delete(testBucket, "$workflowBaseDir/outputs/base64/test-2.b64").execute()

        // Delete optional "none" file for run 3 so we rerun second step for file 3
        googleStorageClient.objects().delete(testBucket, "$workflowBaseDir/outputs/gzip/test-3.b64.fake.none").execute()

        // We're adding grouping of 3 even though we won't have more than 1 task run to run at a time and
        // making sure it still works.
        val executor = runWorkflow((1..3).map { "test-$it.txt" }, grouping = 3, runTimestampOverride = 2)

        // Verify cache is used on operations for file 1
        verifyExecuteWithOutput(executor, "base64/test-1.b64", 0)
        verifyExecuteWithOutput(executor, "gzip/test-1.b64.gz", 0)

        // Verify cache is not used on files 2
        verifyExecuteWithOutput(executor, "base64/test-2.b64")
        verifyExecuteWithOutput(executor, "gzip/test-2.b64.gz", 0)

        // Verify cache is used in only first part of run 3
        verifyExecuteWithOutput(executor, "base64/test-3.b64", 0)
        verifyExecuteWithOutput(executor, "gzip/test-3.b64.gz")
    }

    /**
     * Convenience function that runs the SimpleWorkflow and returns a GoogleLocalExecutor spy
     */
    private fun runWorkflow(inputFiles: List<String>, grouping: Int, runTimestampOverride: Long): GoogleLocalExecutor {
        val configStr = googleConfig(inputFiles, grouping)
        val config = ConfigFactory.parseString(configStr)
        val workflowConfig = createWorkflowConfig(config)
        val executor = spyk(GoogleLocalExecutor(workflowConfig))
        val workflow = gsFilesWorkflow().build(executor, createParamsForConfig(config))
        val taskConfigs = createTaskConfigs(config, workflow)
        val runner = WorkflowRunner(workflow, workflowConfig, taskConfigs, executor, runTimestampOverride)
        runner.run()
        return executor
    }
}

fun writeFileToBucket(bucket: String, filename: String, content: String) {
    val fileMetadata = StorageObject().setName(filename)
    val contentStream = InputStreamContent("text/plain", ByteArrayInputStream(content.toByteArray()))
    googleStorageClient.objects().insert(bucket, fileMetadata, contentStream).execute()
}

fun String.existsInGS(bucket: String, baseDir: String) {
    assertThatCode { googleStorageClient.objects().get(bucket, "$baseDir/$this").execute() }
        .`as`("Object $this should exist in google storage in bucket $bucket under $baseDir")
        .doesNotThrowAnyException()
}

fun String.doesNotExistInGS(bucket: String, baseDir: String) {
    assertThatThrownBy { googleStorageClient.objects().get(bucket, "$baseDir/$this").execute() }
        .`as`("Object $this should not exist in google storage in bucket $bucket under $baseDir")
        .isInstanceOf(IOException::class.java)
}
