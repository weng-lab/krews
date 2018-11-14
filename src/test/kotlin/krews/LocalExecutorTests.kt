package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.matchers.file.shouldExist
import io.kotlintest.specs.StringSpec
import io.mockk.spyk
import io.mockk.verify
import krews.config.createWorkflowConfig
import krews.core.WorkflowRunner
import krews.executor.LocallyDirectedExecutor
import krews.executor.local.LocalExecutor
import krews.file.InputFile
import java.nio.file.Files
import java.nio.file.Paths

class LocalExecutorTests : StringSpec() {
    val tempDir = Paths.get("workflow-test")
    val sampleFilesDir = tempDir.resolve("sample-files-dir")
    val config =
        """
        params {
            sample-files-dir = $sampleFilesDir
        }
        local {
            local-base-dir = $tempDir
        }
        """.trimIndent()

    override fun afterSpec(description: Description, spec: Spec) {
        super.beforeSpec(description, spec)
        //Files.delete(tempDir)

        // Clean up temporary dir
        Files.walk(tempDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }

    init {
        "Can run a simple workflow locally" {
            // Create 3 files in a temp directory to use as inputs.
            Files.createDirectories(sampleFilesDir)
            for (i in 1..3) {
                val file = Files.createFile(sampleFilesDir.resolve("test-$i.txt"))
                Files.write(file, "I am test file #$i".toByteArray())
            }

            val workflow = SimpleWorkflow()
            val workflowConfig = createWorkflowConfig(ConfigFactory.parseString(config), workflow)
            val executor = LocalExecutor(workflowConfig)
            val runner = WorkflowRunner(workflow, workflowConfig, executor, 1)
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

        "Can run a second run on a workflow with with cached inputs and outputs" {
            // Update file #1 and add a new File #4
            Files.write(sampleFilesDir.resolve("test-1.txt"), "I am an updated file".toByteArray())
            val file4 = Files.createFile(sampleFilesDir.resolve("test-4.txt"))
            Files.write(file4, "I am a new file".toByteArray())

            val workflow = SimpleWorkflow()
            val workflowConfig = createWorkflowConfig(ConfigFactory.parseString(config), workflow)
            val executor = spyk(LocalExecutor(workflowConfig))
            val runner = WorkflowRunner(workflow, workflowConfig, executor, 2)
            runner.run()

            val runPath = tempDir.resolve("run/2/")
            val inputsPath = runPath.resolve("inputs")
            for (i in 1..4) {
                inputsPath.resolve("test-$i.txt").shouldExist()
            }

            val outputsPath = runPath.resolve("outputs")
            val base64Path = outputsPath.resolve("base64")
            val gzipPath = outputsPath.resolve("gzip")
            for (i in 1..4) {
                base64Path.resolve("test-$i.b64").shouldExist()
                gzipPath.resolve("test-$i.b64.gz").shouldExist()
            }

            verifyDownloadInputFile(executor, "test-1.txt")
            verifyCachedInputFile(executor, "test-2.txt")
            verifyCachedInputFile(executor, "test-3.txt")
            verifyDownloadInputFile(executor, "test-4.txt")

            // Verify tasks were re-run for test-1 and test-4 and NOT for test-2 and test-3
            verifyExecuteWithOutput(executor, "base64/test-1.b64")
            verifyExecuteWithOutput(executor, "base64/test-2.b64", 0)
            verifyExecuteWithOutput(executor, "base64/test-3.b64", 0)
            verifyExecuteWithOutput(executor, "base64/test-4.b64")

            verifyExecuteWithOutput(executor, "gzip/test-1.b64.gz")
            verifyExecuteWithOutput(executor, "gzip/test-2.b64.gz", 0)
            verifyExecuteWithOutput(executor, "gzip/test-3.b64.gz", 0)
            verifyExecuteWithOutput(executor, "gzip/test-4.b64.gz")
        }
    }
}

private fun verifyCachedInputFile(executorSpy: LocallyDirectedExecutor, path: String) {
    verify {
        executorSpy.copyCachedFiles(any(), any(),
            match { if (it.isEmpty()) false else it.iterator().next() == path })
    }
}

private fun verifyDownloadInputFile(executorSpy: LocallyDirectedExecutor, path: String) {
    verify {
        executorSpy.executeTask(any(), any(), any(), any(), any(), any(), any(), any(), setOf(),
            match { if (it.isEmpty()) false else it.iterator().next().path == path })
    }
}

private fun verifyExecuteWithOutput(executorSpy: LocallyDirectedExecutor, path: String, times: Int = 1) {
    verify(exactly = times) {
        executorSpy.executeTask(any(), any(), any(), any(), any(), any(), any(),
            match { if (it.isEmpty()) false else it.iterator().next().path == path },
            any(), any())
    }
}