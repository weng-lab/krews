package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.specs.StringSpec
import krews.config.createWorkflowConfig
import krews.executor.GoogleExecutor

val googleConfig =
    """
    google-exec {
        storage-bucket = "wenglab-data-common"
        storage-base-dir = "workflow-tests"
    }
    """.trimIndent()

class GoogleExecutorTests : StringSpec({
    "Can run a simple workflow locally" {
        val workflowConfig = createWorkflowConfig(ConfigFactory.parseString(googleConfig), SimpleWorkflow)
        val executor = GoogleExecutor(workflowConfig)
        val runner = WorkflowRunner(SimpleWorkflow, workflowConfig, executor)
        runner.run()
    }
})