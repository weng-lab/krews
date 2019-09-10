package krews

import com.typesafe.config.ConfigFactory
import io.mockk.spyk
import krews.config.*
import krews.core.*
import krews.executor.slurm.SlurmExecutor
import krews.util.*
import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.nio.file.*
import kotlin.streams.toList

private val log = KotlinLogging.logger {}

/*
 * The following need to be run in preparation of these tests:
 *
 * ssh -L 12222:z018:22 brooksj@z018 -N
 * sudo sshfs -o allow_other,defer_permissions brooksj@localhost:/data/zusers.ds/brooksj /data/zusers/brooksj -p 12222
 */

@Disabled
class SlurmExecutorTests {

    private val testDir = Paths.get("/data/zusers/brooksj/slurm-workflow-test")!!
    private val sampleFilesDir = testDir.resolve("sample-files")
    private val inputsDir = testDir.resolve("inputs")
    private val outputsDir = testDir.resolve("outputs")
    private val base64Dir = outputsDir.resolve("base64")
    private val gzipDir = outputsDir.resolve("gzip")

    private fun config(taskParam: String) =
        """
        working-dir = $testDir
        slurm {
            ssh {
                user = brooksj
                host = localhost
                port = 12222
            }
        }
        params {
            sample-files-dir = $sampleFilesDir
        }
        task.default {
            grouping = 2
        }
        task.base64 {
            params = {
                some-val = $taskParam
            }
            slurm.sbatch-args {
                comment = "\"This is just a test.\""
            }
        }

        """.trimIndent()

    @BeforeAll
    fun beforeTests() {
        if (Files.isDirectory(testDir)) {
            Files.walk(testDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.delete(it) }
        }
        Files.createDirectories(sampleFilesDir)
    }

    @Test
    fun `Can run a simple workflow on slurm`() {
        // Create 3 files in a temp directory to use as inputs.
        for (i in 1..3) {
            val file = Files.createFile(sampleFilesDir.resolve("test-$i.txt"))
            Files.write(file, "I am test file #$i".toByteArray())
        }

        runWorkflow(1, "task-param-1")

        for (i in 1..3) {
            assertThat(base64Dir.resolve("test-$i.b64")).exists()
            assertThat(gzipDir.resolve("test-$i.b64.gz")).exists()

            // Confirm that logs and an html report were generated
            val runPath = testDir.resolve("run/1/")
            val logsDirs = Files.list(runPath.resolve(LOGS_DIR)).toList()
            assertThat(logsDirs.size).isEqualTo(4)
            assertThat(logsDirs[0].resolve("out.txt")).exists()
            assertThat(runPath.resolve(REPORT_FILENAME)).exists()
        }
    }

    /**
     * Convenience function that runs the SimpleWorkflow and returns a LocalExecutor spy
     */
    private fun runWorkflow(runTimestampOverride: Long, taskParam: String): SlurmExecutor {
        val parsedConfig = ConfigFactory.parseString(config(taskParam))
        val workflowConfig = createWorkflowConfig(parsedConfig)
        val executor = spyk(SlurmExecutor(workflowConfig))
        val workflow = localFilesWorkflow().build(executor, createParamsForConfig(parsedConfig))
        val taskConfigs = createTaskConfigs(parsedConfig, workflow)

        val runner = WorkflowRunner(workflow, workflowConfig, taskConfigs, executor, runTimestampOverride)
        runner.run()
        return executor
    }
}