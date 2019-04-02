package krews

import com.typesafe.config.ConfigFactory
import io.mockk.spyk
import krews.config.*
import krews.core.WorkflowRunner
import krews.executor.*
import krews.executor.local.LocalExecutor
import krews.util.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.nio.file.*
import kotlin.streams.toList

@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalExecutorTests {
    private val testDir = Paths.get("local-workflow-test")!!
    private val inputsDir = testDir.resolve("inputs")
    private val outputsDir = testDir.resolve("outputs")
    private val sampleFilesDir = testDir.resolve("sample-files")
    private val unusedFilesDir = testDir.resolve("unused-files")
    private val base64Dir = outputsDir.resolve("base64")
    private val gzipDir = outputsDir.resolve("gzip")

    private fun config(taskParam: String) =
        """
        local-files-base-dir = $testDir
        clean-old-files = true
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
                    cache = true
                },
                {
                    -type = "krews.file.LocalInputFile"
                    local-path = $unusedFilesDir/unused-2.txt
                    path = unused-2.txt
                }
            ]
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

    @Test
    fun `Can run a simple workflow locally`() {
        // Create 3 files in a temp directory to use as inputs.
        for (i in 1..3) {
            val file = Files.createFile(sampleFilesDir.resolve("test-$i.txt"))
            Files.write(file, "I am test file #$i".toByteArray())
        }

        val executor = runWorkflow(1, "task-param-1")

        val dbPath = testDir.resolve(Paths.get("state", "metadata.db"))
        assertThat(dbPath).exists()

        assertThat(inputsDir.resolve("unused-1.txt")).exists()
        assertThat(inputsDir.resolve("unused-2.txt")).doesNotExist()

        // Even though this file was required for each task, make sure it only tried to download once.
        verifyInputFileCached(executor, "unused-1.txt", 1)
        // This file is downloaded by each task execution, not cached
        verifyInputFileCached(executor, "unused-2.txt", 0)

        for (i in 1..3) {
            assertThat(inputsDir.resolve("test-$i.txt")).exists()
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
            verifyInputFileCached(executor, "test-$i.txt")
        }

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/1/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(6)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    @Test
    fun `Can invalidate cache using different task parameters`() {
        val executor = runWorkflow(2, "task-param-2")

        for (i in 1..3) {
            assertThat(inputsDir.resolve("test-$i.txt")).exists()
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
            verifyInputFileCached(executor, "test-$i.txt", 0)
            verifyExecuteWithOutput(executor, "base64/test-$i.b64")
            verifyExecuteWithOutput(executor, "gzip/test-$i.b64.gz")
        }

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/2/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(6)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    @Test
    fun `Should cache unless invalidated by modified input file`() {
        // Update file #1 and add a new File #4
        Files.delete(sampleFilesDir.resolve("test-1.txt"))
        Files.write(sampleFilesDir.resolve("test-3.txt"), "I am an updated file".toByteArray())
        val file4 = Files.createFile(sampleFilesDir.resolve("test-4.txt"))
        Files.write(file4, "I am a new file".toByteArray())

        val executor = runWorkflow(3, "task-param-2")

        for (i in 2..4) {
            assertThat(inputsDir.resolve("test-$i.txt")).exists()
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
        }

        // Since test-1.txt is not an input and cleaning old files is on, make sure test-1 input and output files don't exist
        assertThat(inputsDir.resolve("test-1.txt")).doesNotExist()
        assertThat(base64Dir.resolve("test-1.b64")).doesNotExist()
        assertThat(gzipDir.resolve("test-1.b64.gz")).doesNotExist()

        verifyInputFileCached(executor, "test-2.txt", 0)
        verifyInputFileCached(executor, "test-3.txt")
        verifyInputFileCached(executor, "test-4.txt")

        // Verify tasks were re-run for test-3 and test-4 and NOT for test-2
        verifyExecuteWithOutput(executor, "base64/test-2.b64", 0)
        verifyExecuteWithOutput(executor, "base64/test-3.b64")
        verifyExecuteWithOutput(executor, "base64/test-4.b64")

        verifyExecuteWithOutput(executor, "gzip/test-2.b64.gz", 0)
        verifyExecuteWithOutput(executor, "gzip/test-3.b64.gz")
        verifyExecuteWithOutput(executor, "gzip/test-4.b64.gz")

        // Confirm that logs and an html report were generated
        val runPath = testDir.resolve("run/3/")
        assertThat(Files.list(runPath.resolve(LOGS_DIR)).toList().size).isEqualTo(4)
        assertThat(runPath.resolve(REPORT_FILENAME)).exists()
    }

    @Test
    fun `Cache should be invalidated if input file or output file is removed manually`() {
        Files.delete(outputsDir.resolve("base64/test-2.b64"))
        Files.delete(inputsDir.resolve("test-3.txt"))

        val executor = runWorkflow(4, "task-param-2")

        for (i in 2..4) {
            assertThat(inputsDir.resolve("test-$i.txt")).exists()
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()
        }

        verifyInputFileCached(executor, "test-2.txt", 0)
        verifyInputFileCached(executor, "test-3.txt")
        verifyInputFileCached(executor, "test-4.txt", 0)

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