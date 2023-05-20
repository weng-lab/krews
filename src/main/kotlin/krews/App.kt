package krews

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.choice
import com.typesafe.config.ConfigFactory
import krews.config.*
import krews.core.*
import krews.executor.google.*
import krews.executor.local.LocalExecutor
import krews.executor.slurm.SlurmExecutor
import mu.KotlinLogging
import java.io.File
import java.nio.file.*


private val log = KotlinLogging.logger {}

// The name of an environment variable used for overriding the timestamp associated with used for workflow runs
// Required for remote executors logging. Should never be used directly by users.
const val WORKFLOW_RUN_TIMESTAMP_ENV_VAR = "KREWS_RUN_TIMESTAMP_OVERRIDE"

fun run(workflow: WorkflowBuilder, args: Array<String>) {
    KrewsApp(workflow).main(args)
}

class KrewsApp(private val workflowBuilder: WorkflowBuilder) : CliktCommand() {

    private val on by option("-o", "--on", help = "where the workflow will run")
        .choice(
            "local" to Executors.LOCAL,
            "google" to Executors.GOOGLE,
            "google-local" to Executors.GOOGLE_LOCAL,
            "slurm" to Executors.SLURM,
            "bsub" to Executors.BSUB
        )
        .default(Executors.LOCAL)
    private val executable by option("-e", "--executable",
        help = "reference to krews app as an executable. Used to run remotely. Not required for fat jars.")
        .convert { Paths.get(it) }
    private val config by option("-c", "--config")
        .convert { Paths.get(it) }

    override fun run() {
        if (config != null && !Files.exists(config)) throw Exception("Given config $config not found")
        val hoconConfig = if (config != null) ConfigFactory.parseFile(config!!.toFile()) else ConfigFactory.empty()
        val params = createParamsForConfig(hoconConfig)
        val workflowConfig = createWorkflowConfig(hoconConfig)

        if (on.locallyDirected) {
            val executor = when(on) {
                Executors.LOCAL -> LocalExecutor(workflowConfig)
                Executors.GOOGLE_LOCAL -> GoogleLocalExecutor(workflowConfig)
                Executors.SLURM -> SlurmExecutor(workflowConfig)
                Executors.BSUB -> BsubExecutor(workflowConfig)
                else -> throw Exception("Unsupported executor")
            }
            val workflow = workflowBuilder.build(executor, params)

            val maxHeapSize = Runtime.getRuntime().maxMemory()
            log.info { "Krews running with max heap size $maxHeapSize" }

            val runTimestampOverride = System.getenv(WORKFLOW_RUN_TIMESTAMP_ENV_VAR)?.toLong()
            val taskConfigs = createTaskConfigs(hoconConfig, workflow)
            val runner = WorkflowRunner(workflow, workflowConfig, taskConfigs, executor, runTimestampOverride)

            // Add shutdown hook to stop all running tasks and other cleanup work if master is stopped.
            Runtime.getRuntime().addShutdownHook(Thread {
                log.info { "Shutdown detecting. Cleaning up..." }
                executor.shutdownRunningTasks()
                runner.onShutdown()
            })

            runner.run()
        } else {
            val executor = when(on) {
                Executors.GOOGLE -> GoogleExecutor(workflowConfig)
                else -> throw Exception("Unsupported executor")
            }
            val executable = executable ?: Paths.get(File(KrewsApp::class.java.protectionDomain.codeSource.location.toURI()).path)
            val config = checkNotNull(config) { "--config required for remote executors" }
            executor.execute(executable, config)
        }
    }
}

enum class Executors(val locallyDirected: Boolean) {
    LOCAL(true), GOOGLE(false), GOOGLE_LOCAL(true), SLURM(true)
}
