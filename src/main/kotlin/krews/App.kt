package krews

import com.typesafe.config.ConfigFactory
import krews.config.createWorkflowConfig

fun run(workflowName: String, args: List<String>) {
    workflow.name = workflowName
    run(workflow, args)
}

fun run(workflow: Workflow, args: List<String>) {
    //val
    val workflowConfig = createWorkflowConfig(ConfigFactory.parseResources("local-exec.conf"), workflow)
    val runner = WorkflowRunner(workflow, workflowConfig)
    runner.run()
}