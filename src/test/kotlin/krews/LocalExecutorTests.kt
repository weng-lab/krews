package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.specs.StringSpec
import krews.config.createWorkflowConfig

class LocalExecutorTests : StringSpec({
    "Can run a simple workflow locally" {
        val workflowConfig = createWorkflowConfig(ConfigFactory.empty(), SimpleWorkflow)
        val runner = WorkflowRunner(SimpleWorkflow, workflowConfig)
        runner.run()
    }
})