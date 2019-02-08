package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.*
import io.kotlintest.matchers.file.shouldExist
import io.kotlintest.matchers.file.shouldNotExist
import io.kotlintest.specs.StringSpec
import io.mockk.spyk
import krews.config.SshConfig
import krews.config.createParamsForConfig
import krews.config.createWorkflowConfig
import krews.core.WorkflowRunner
import krews.executor.LOGS_DIR
import krews.executor.REPORT_FILENAME
import krews.executor.google.googleStorageClient
import krews.executor.local.LocalExecutor
import krews.executor.slurm.SlurmExecutor
import krews.misc.CommandExecutor
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.streams.toList

private val log = KotlinLogging.logger {}

/*
 * The following need to be run in preparation of these tests:
 *
 * ssh -L 12222:z018:22 brooksj@z018 -N
 * sudo sshfs -o allow_other,defer_permissions brooksj@localhost:/data/zusers.ds/brooksj /data/zusers/brooksj -p 12222
 */

class SlurmExecutorTests : StringSpec() {
    override fun tags() = setOf(E2E)

    private val testDir = Paths.get("/data/zusers/brooksj/slurm-workflow-test")!!
    private val sampleFilesDir = testDir.resolve("sample-files")
    private val inputsDir = testDir.resolve("inputs")
    private val outputsDir = testDir.resolve("outputs")
    private val base64Dir = outputsDir.resolve("base64")
    private val gzipDir = outputsDir.resolve("gzip")

    private fun config(taskParam: String) =
        """
        local-files-base-dir = $testDir
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
        task.base64.params = {
            some-val = $taskParam
        }
        """.trimIndent()

    override fun beforeSpec(description: Description, spec: Spec) {
        Files.walk(testDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
        Files.createDirectories(sampleFilesDir)
    }

    init {
        "Can run a simple workflow on slurm" {
            // Create 3 files in a temp directory to use as inputs.
            for (i in 1..3) {
                val file = Files.createFile(sampleFilesDir.resolve("test-$i.txt"))
                Files.write(file, "I am test file #$i".toByteArray())
            }

            val executor = runWorkflow(1, "task-param-1")

            for (i in 1..3) {
                inputsDir.resolve("test-$i.txt").shouldExist()
                base64Dir.resolve("test-$i.b64").shouldExist()
                gzipDir.resolve("test-$i.b64.gz").shouldExist()
                verifyInputFileCached(executor, "test-$i.txt")

                // Confirm that logs and an html report were generated
                val runPath = testDir.resolve("run/1/")
                val logsDirs = Files.list(runPath.resolve(LOGS_DIR)).toList()
                logsDirs.size shouldBe 6
                logsDirs[0].resolve("out.txt").shouldExist()
                runPath.resolve(REPORT_FILENAME).shouldExist()
            }
        }
    }

    /**
     * Convenience function that runs the SimpleWorkflow and returns a LocalExecutor spy
     */
    private fun runWorkflow(runTimestampOverride: Long, taskParam: String): SlurmExecutor {
        val parsedConfig = ConfigFactory.parseString(config(taskParam))
        val workflow = localFilesWorkflow().build(createParamsForConfig(parsedConfig))
        val workflowConfig = createWorkflowConfig(parsedConfig, workflow)
        val executor = spyk(SlurmExecutor(workflowConfig))
        val runner = WorkflowRunner(workflow, workflowConfig, executor, runTimestampOverride)
        runner.run()
        return executor
    }
}