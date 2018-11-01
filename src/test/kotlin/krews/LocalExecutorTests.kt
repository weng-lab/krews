package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.specs.StringSpec
import krews.config.createWorkflowConfig
import krews.executor.local.LocalExecutor

class LocalExecutorTests : StringSpec({
    "Can run a simple workflow locally" {
        val workflowConfig = createWorkflowConfig(ConfigFactory.empty(), SimpleWorkflow)
        val executor = LocalExecutor(workflowConfig)
        val runner = WorkflowRunner(SimpleWorkflow, workflowConfig, executor)
        runner.run()
    }
})