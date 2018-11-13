package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.matchers.file.shouldExist
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import krews.config.createWorkflowConfig
import krews.core.WorkflowRunner
import krews.executor.local.LocalExecutor
import java.nio.file.Files
import java.nio.file.Paths

class LocalExecutorTests : StringSpec() {
    val tempDir = Files.createDirectories(Paths.get("workflow-test"))
    val config =
        """
        local {
            local-base-dir = $tempDir
        }
        """.trimIndent()

    /*override fun afterSpec(description: Description, spec: Spec) {
        super.beforeSpec(description, spec)
        Files.delete(tempDir)
    }*/

    init {
        "Can run a simple workflow locally" {
            val workflowConfig = createWorkflowConfig(ConfigFactory.parseString(config), SimpleWorkflow)
            val executor = LocalExecutor(workflowConfig)
            val runner = WorkflowRunner(SimpleWorkflow, workflowConfig, executor, 1)
            runner.run()

            val dbPath = tempDir.resolve(Paths.get("state", "metadata.db"))
            dbPath.shouldExist()

            val runPath = tempDir.resolve("run/1/")
            val inputsPath = runPath.resolve("inputs")
            for (i in 1..3) {
                inputsPath.resolve("test-$i.txt").shouldExist()
            }

            val outputsPath = runPath.resolve("outputs")
            val base64Path = outputsPath.resolve("base64")
            val gzipPath = outputsPath.resolve("gzip")
            for (i in 1..3) {
                base64Path.resolve("test-$i.b64").shouldExist()
                gzipPath.resolve("test-$i.b64.gz").shouldExist()
            }
        }
    }
}