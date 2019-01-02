package krews.executor.google

import com.google.api.services.genomics.v2alpha1.model.*
import krews.WORKFLOW_RUN_TIMESTAMP_ENV_VAR
import krews.config.WorkflowConfig
import krews.executor.LOGS_DIR
import krews.executor.REPORT_FILENAME
import krews.executor.RUN_DIR
import krews.executor.RemoteDirectedExecutor
import mu.KotlinLogging
import org.joda.time.DateTime
import java.nio.file.Path

private val log = KotlinLogging.logger {}

const val REMOTE_BIN_DIR = "bin"
const val DIRECTOR_MACHINE_TYPE = "n1-standard-1"
const val MASTER_LOG_DIR = "master"
const val MASTER_RUN_DIR = "/app"
const val MASTER_IMAGE = "openjdk:8"

class GoogleExecutor(workflowConfig: WorkflowConfig) : RemoteDirectedExecutor {

    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir


    override fun execute(executablePath: Path, configPath: Path) {
        val workflowTime = DateTime.now()

        val executableFilename = executablePath.fileName!!.toString()
        val executableObject = gcsObjectPath(gcsBase, RUN_DIR, workflowTime.millis.toString(), REMOTE_BIN_DIR, executableFilename)
        val configFilename = configPath.fileName!!.toString()
        val configObject = gcsObjectPath(gcsBase, RUN_DIR, workflowTime.millis.toString(), REMOTE_BIN_DIR, configFilename)

        uploadObject(googleStorageClient, bucket, executableObject, executablePath)
        uploadObject(googleStorageClient, bucket, configObject, configPath)

        val run = RunPipelineRequest()
        val pipeline = Pipeline()
        run.pipeline = pipeline

        val resources = Resources()
        pipeline.resources = resources
        if (!googleConfig.zones.isEmpty()) {
            resources.zones = googleConfig.zones
        } else if (!googleConfig.regions.isEmpty()) {
            resources.regions = googleConfig.regions
        }

        resources.projectId = googleConfig.projectId

        val virtualMachine = VirtualMachine()
        resources.virtualMachine = virtualMachine
        virtualMachine.machineType = DIRECTOR_MACHINE_TYPE

        val disk = Disk()
        virtualMachine.disks = listOf(disk)
        disk.name = DISK_NAME

        val serviceAccount = ServiceAccount()
        virtualMachine.serviceAccount = serviceAccount
        serviceAccount.scopes = listOf(STORAGE_READ_WRITE_SCOPE)

        val actions = mutableListOf<Action>()
        pipeline.actions = actions

        val runObjectPath = gcsObjectPath(gcsBase, RUN_DIR, workflowTime.millis.toString())
        val runPath = gcsPath(bucket, runObjectPath)

        // Create action to periodically copy logs to GCS
        val logPath = gcsPath(bucket, runObjectPath, LOGS_DIR, MASTER_LOG_DIR, LOG_FILE_NAME)
        actions.add(createPeriodicLogsAction(logPath, googleConfig.logUploadInterval))

        // Create action to download executable
        actions.add(createDownloadAction(gcsPath(bucket, executableObject), MASTER_RUN_DIR, executableFilename))

        // Create action to download config
        actions.add(createDownloadAction(gcsPath(bucket, configObject), MASTER_RUN_DIR, configFilename))

        // Create action to execute krews workflow
        actions.add(createExecuteWorkflowAction(executableFilename, configFilename, workflowTime))

        // Create action to copy logs to GCS after everything else is complete
        actions.add(createLogsAction(logPath))

        log.info { "Submitting pipeline job for workflow run with timestamp ${workflowTime.millis}:\n$run" }
        googleGenomicsClient.projects().operations()
        val initialOp: Operation = googleGenomicsClient.pipelines().run(run).execute()

        val reportPath = gcsPath(bucket, bucket, runObjectPath, REPORT_FILENAME)
        log.info {
            """
            |Master pipeline job submitted
            |See operation: "${initialOp.name}".
            |
            |All results for this run will be under $runPath
            |
            |You can check on the status of the job by checking:
            |    - The status report at $reportPath
            |    - The master logs at $logPath
            |""".trimMargin()
        }
    }
}

/**
 * Create a pipeline action that will execute the workflow (via the given krews executable)
 */
internal fun createExecuteWorkflowAction(executableFilename: String, configFilename: String, workflowTime: DateTime): Action {
    val action = Action()
    action.imageUri = MASTER_IMAGE
    action.mounts = listOf(createMount(MASTER_RUN_DIR))
    action.environment = mapOf(WORKFLOW_RUN_TIMESTAMP_ENV_VAR to "${workflowTime.millis}")
    val cmd =
        """
        set -x
        ls -l $MASTER_RUN_DIR
        ${if (executableFilename.endsWith(".jar")) "java -jar " else ""}$MASTER_RUN_DIR/$executableFilename \
            --on google-local \
            --config $MASTER_RUN_DIR/$configFilename
        """.trimIndent()
    action.commands = listOf("sh", "-c", cmd)
    return action
}