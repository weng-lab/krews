package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.specs.StringSpec
import krews.config.createWorkflowConfig
import krews.executor.GoogleExecutor

val googleConfig =
    """
    google {
        storage-bucket = "wenglab-data-common"
        storage-base-dir = "workflow-tests"
        local-storage-base-dir = "workflow-out"
        project-id = "devenv-215523"
        regions = ["us-east1", "us-east4"]
        job-completion-poll-interval = 5
        log-upload-interval = 5
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