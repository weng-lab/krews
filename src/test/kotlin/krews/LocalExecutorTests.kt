package krews

import com.typesafe.config.ConfigFactory
import io.mockk.spyk
import krews.config.*
import krews.core.*
import krews.executor.local.LocalExecutor
import krews.util.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.nio.file.*
import kotlin.streams.toList

@Disabled
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class LocalExecutorTests {
    private val testDir = Paths.get("local-workflow-test")!!
    private val outputsDir = testDir.resolve("outputs")
    private val sampleFilesDir = testDir.resolve("sample-files")
    private val unusedFilesDir = testDir.resolve("unused-files")
    private val base64Dir = outputsDir.resolve("base64")
    private val gzipDir = outputsDir.resolve("gzip")

    private fun config(taskParam: String) =
        """
        working-dir = $testDir
        params {
            sample-files-dir = $sampleFilesDir
        }
        task.base64.params = {
            some-val = $taskParam
            some-files = [
                {
                    -type = "krews.file.LocalInputFile"
                    local-path = $unusedFilesDir/unused-1.txt
                    path = unused-1.txt
                },
                {
                    -type = "krews.file.LocalInputFile"
                    local-path = $unusedFilesDir/unused-2.txt
                    path = unused-2.txt
                }
            ]
        }
        task.default {
            grouping = 2
        }
        """.trimIndent()

    @BeforeAll
    fun beforeTests() {
        deleteDir(testDir)

        // Create temp sample files dir (and parent test dir) to use for this set of tests
        Files.createDirectories(sampleFilesDir)
        createFile(unusedFilesDir.resolve("unused-1.txt"), "I am an unused task input file")
        createFile(unusedFilesDir.resolve("unused-2.txt"), "I am an unused task input file")
    }

    @AfterAll
    fun afterTests() = deleteDir(testDir)

    @Test @Order(1)
    fun `Can run a simple workflow locally`() {
        // Create 3 files in a temp directory to use as inputs.
        for (i in 1..3) {
            val file = Files.createFile(sampleFilesDir.resolve("test-$i.txt"))
            Files.write(file, "I am test file #$i".toByteArray())
        }

        runWorkflow(1, "task-param-1")

        val dbPath = testDir.resolve(Paths.get("state", "cache.db"))
        assertThat(dbPath).exists()

        for (i in 1..3) {
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
        }

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/1/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(6)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    @Test @Order(2)
    fun `Can invalidate cache using different task parameters`() {
        val executor = runWorkflow(2, "task-param-2")

        for (i in 1..3) {
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
            verifyExecuteWithOutput(executor, "base64/test-$i.b64")
            verifyExecuteWithOutput(executor, "gzip/test-$i.b64.gz")
        }

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/2/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(6)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    @Test @Order(3)
    fun `Should cache unless invalidated by modified input file`() {
        // Update file #1 and add a new File #4
        Files.delete(sampleFilesDir.resolve("test-1.txt"))
        Files.write(sampleFilesDir.resolve("test-3.txt"), "I am an updated file".toByteArray())
        val file4 = Files.createFile(sampleFilesDir.resolve("test-4.txt"))
        Files.write(file4, "I am a new file".toByteArray())

        val executor = runWorkflow(3, "task-param-2")

        // Verify output files 1 to 4 exist. "1" files should exist from previous run.
        for (i in 1..4) {
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
        }

        // Verify tasks were re-run for test-3 and test-4 and NOT for test-1 and test-2
        verifyExecuteWithOutput(executor, "base64/test-1.b64", 0)
        verifyExecuteWithOutput(executor, "base64/test-2.b64", 0)
        verifyExecuteWithOutput(executor, "base64/test-3.b64")
        verifyExecuteWithOutput(executor, "base64/test-4.b64")

        verifyExecuteWithOutput(executor, "gzip/test-1.b64.gz", 0)
        verifyExecuteWithOutput(executor, "gzip/test-2.b64.gz", 0)
        verifyExecuteWithOutput(executor, "gzip/test-3.b64.gz")
        verifyExecuteWithOutput(executor, "gzip/test-4.b64.gz")

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/3/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(4)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    @Test @Order(4)
    fun `Cache should be invalidated if output file is removed manually`() {
        Files.delete(outputsDir.resolve("base64/test-2.b64"))
        val executor = runWorkflow(4, "task-param-2")

        for (i in 2..4) {
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
        }

        verifyExecuteWithOutput(executor, "base64/test-2.b64")
        // Although it downloads the input file again, it doesn't need to reprocess the task
        // because it's source is the same file with the same last mod date
        verifyExecuteWithOutput(executor, "base64/test-3.b64", 0)
        verifyExecuteWithOutput(executor, "base64/test-4.b64", 0)

        verifyExecuteWithOutput(executor, "gzip/test-2.b64.gz")
        verifyExecuteWithOutput(executor, "gzip/test-3.b64.gz", 0)
        verifyExecuteWithOutput(executor, "gzip/test-4.b64.gz", 0)

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/4/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(2)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    /**
     * Convenience function that runs the SimpleWorkflow and returns a LocalExecutor spy
     */
    private fun runWorkflow(runTimestampOverride: Long, taskParam: String): LocalExecutor {
        val parsedConfig = ConfigFactory.parseString(config(taskParam))
        val workflow = localFilesWorkflow().build(createParamsForConfig(parsedConfig))
        val workflowConfig = createWorkflowConfig(parsedConfig, workflow)
        val executor = spyk(LocalExecutor(workflowConfig))
        val runner = WorkflowRunner(workflow, workflowConfig, executor, runTimestampOverride)
        runner.run()
        return executor
    }
}