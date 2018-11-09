package krews

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import com.typesafe.config.ConfigFactory
import krews.config.createWorkflowConfig
import krews.core.Workflow
import krews.core.WorkflowRunner
import krews.core.defaultWorkflow
import krews.executor.google.GoogleExecutor
import krews.executor.google.GoogleLocalExecutor
import krews.executor.local.LocalExecutor
import java.nio.file.Paths


// The name of an environment variable used for overriding the timestamp associated with used for workflow runs
// Required for remote executors logging. Should never be used directly by users.
const val WORKFLOW_RUN_TIMESTAMP_ENV_VAR = "KREWS_RUN_TIMESTAMP_OVERRIDE"

fun run(workflowName: String, args: Array<String>) {
    defaultWorkflow.name = workflowName
    run(defaultWorkflow, args)
}

fun run(workflow: Workflow, args: Array<String>) {
    KrewsApp(workflow).main(args)
}

class KrewsApp(private val workflow: Workflow) : CliktCommand() {

    private val on by option("-o", "--on", help = "where the workflow will run")
        .choice(
            "local" to Executors.LOCAL,
            "google" to Executors.GOOGLE,
            "google-local" to Executors.GOOGLE_LOCAL
        )
        .default(Executors.LOCAL)
    private val executable by option("-e", "--executable", help = "reference to krews app as an executable. Used to run remotely.")
        .convert { Paths.get(it) }
    private val config by option("-c", "--config")
        .convert { Paths.get(it) }

    override fun run() {
        val hoconConfig = if (config != null) ConfigFactory.parseFile(config!!.toFile()) else ConfigFactory.empty()
        val workflowConfig = createWorkflowConfig(hoconConfig, workflow)

        if (on.locallyDirected) {
            val executor = when(on) {
                Executors.LOCAL -> LocalExecutor(workflowConfig)
                Executors.GOOGLE_LOCAL -> GoogleLocalExecutor(workflowConfig)
                else -> throw Exception("Unsupported executor")
            }

            val runTimestampOverride = System.getenv(WORKFLOW_RUN_TIMESTAMP_ENV_VAR)?.toLong()
            val runner = WorkflowRunner(workflow, workflowConfig, executor, runTimestampOverride)
            runner.run()
        } else {
            val executor = when(on) {
                Executors.GOOGLE -> GoogleExecutor(workflowConfig)
                else -> throw Exception("Unsupported executor")
            }
            val executable = checkNotNull(executable) { "--executable required for remote executors" }
            val config = checkNotNull(config) { "--config required for remote executors" }
            executor.execute(executable, config)
        }
    }
}

enum class Executors(val locallyDirected: Boolean) {
    LOCAL(true), GOOGLE(false), GOOGLE_LOCAL(true)
}
