package krews.executor.google

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.genomics.v2alpha1.Genomics
import com.google.api.services.genomics.v2alpha1.model.*
import com.google.api.services.storage.Storage
import krews.WFile
import krews.config.CapacityType
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.executor.*
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Paths


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"

// Default VM machine type if not define in task configuration
const val DEFAULT_MACHINE_TYPE = "n1-standard-1"

class GoogleLocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val genomicsClient: Genomics
    private val storageClient: Storage
    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Local Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir
    private val dbFilePath = Paths.get(googleConfig.localStorageBaseDir, DB_FILENAME).toAbsolutePath()
    private val dbStorageObject = gcsObjectPath(gcsBase, STATE_DIR, DB_FILENAME)

    init {
        val transport = NetHttpTransport()
        val jsonFactory = JacksonFactory.getDefaultInstance()
        val credentials = GoogleCredential.getApplicationDefault()
        genomicsClient = Genomics.Builder(transport, jsonFactory, credentials)
            .setApplicationName(APPLICATION_NAME)
            .build()
        storageClient = Storage.Builder(transport, jsonFactory, credentials)
            .setApplicationName(APPLICATION_NAME)
            .build()
    }

    override fun prepareDatabaseFile(): String {
        log.info { "Deleting local copy of $dbFilePath if it exists" }
        Files.deleteIfExists(dbFilePath)

        log.info { "Attempting to download $dbStorageObject from bucket $bucket..." }
        val fileExists = downloadObject(storageClient, bucket, dbStorageObject, dbFilePath)
        if (fileExists) {
            log.info { "$dbStorageObject not found in bucket $bucket. A new database file will be used." }
        } else {
            log.info { "$dbStorageObject successfully downloaded to $dbFilePath" }
        }
        return dbFilePath.toString()
    }

    override fun pushDatabaseFile() {
        log.info { "Pushing database file $dbFilePath to object $dbStorageObject in bucket $bucket" }
        uploadObject(storageClient, bucket, dbStorageObject, dbFilePath)
    }

    override fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<WFile>) {
        log.info { "Copying cached outputs $outputFiles from workflow run dir $fromWorkflowDir to $toWorkflowDir" }
        outputFiles.forEach { outputFile ->
            val fromObject = gcsObjectPath(gcsBase, RUN_DIR, fromWorkflowDir, OUTPUTS_DIR, outputFile.path)
            val toObject = gcsObjectPath(gcsBase, RUN_DIR, toWorkflowDir, OUTPUTS_DIR, outputFile.path)
            copyObject(storageClient, bucket, fromObject, bucket, toObject)
        }
    }

    override fun executeTask(workflowRunDir: String, taskRunId: Int, taskConfig: TaskConfig, dockerImage: String,
                             dockerDataDir: String, command: String?, inputItem: Any, outputItem: Any?) {
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
        virtualMachine.machineType = taskConfig.google?.machineType ?: DEFAULT_MACHINE_TYPE

        val serviceAccount = ServiceAccount()
        virtualMachine.serviceAccount = serviceAccount
        serviceAccount.scopes = listOf(STORAGE_READ_WRITE_SCOPE)

        val disk = Disk()
        virtualMachine.disks = listOf(disk)
        disk.name = DISK_NAME
        if (taskConfig.google?.diskSize != null) {
            disk.sizeGb = taskConfig.google.diskSize.toType(CapacityType.GB).toInt()
        }

        val actions = mutableListOf<Action>()
        pipeline.actions = actions

        // Create action to periodically copy logs to GCS
        val logPath = gcsPath(bucket, gcsBase, RUN_DIR, workflowRunDir, LOGS_DIR, taskRunId.toString(), LOG_FILE_NAME)
        actions.add(createPeriodicLogsAction(logPath, googleConfig.logUploadInterval))

        // Create actions to download each input file
        val inputFiles = getFilesForObject(inputItem)
        val downloadActions = inputFiles.map {
            val inputFileObject = gcsPath(bucket, gcsBase, RUN_DIR, workflowRunDir, OUTPUTS_DIR, it.path)
            createDownloadAction(inputFileObject, dockerDataDir, it.path)
        }
        actions.addAll(downloadActions)

        // Create the action that runs the task
        actions.add(createExecuteTaskAction(dockerImage, dockerDataDir, command))

        // Create the actions to upload each output file
        val outputFiles = getFilesForObject(outputItem)
        val uploadActions = outputFiles.map {
            val outputFileObject = gcsPath(bucket, gcsBase, RUN_DIR, workflowRunDir, OUTPUTS_DIR, it.path)
            createUploadAction(outputFileObject, dockerDataDir, it.path)
        }
        actions.addAll(uploadActions)

        // Create action to copy logs to GCS after everything else is complete
        actions.add(createLogsAction(logPath))

        log.info { "Submitting pipeline job for task run $taskRunId:\n$run" }
        genomicsClient.projects().operations()
        val initialOp: Operation = genomicsClient.pipelines().run(run).execute()

        log.info { "Pipeline job submitted. Operation returned: \"${initialOp.name}\". " +
                "Will check for completion every ${googleConfig.jobCompletionPollInterval} seconds" }
        do {
            Thread.sleep(googleConfig.jobCompletionPollInterval * 1000L)
            val op: Operation = genomicsClient.projects().operations().get(initialOp.name).execute()
            if (op.done) {
                log.info { "Pipeline job \"${op.name}\" complete! Complete results: ${op.toPrettyString()}" }
            } else {
                log.info { "Pipeline job \"${op.name}\" still running..." }
            }
        } while (!op.done)
    }

}

/**
 * Create a pipeline action that will execute the task
 */
internal fun createExecuteTaskAction(dockerImage: String, dataDir: String, command: String?): Action {
    val action = Action()
    action.imageUri = dockerImage
    action.mounts = listOf(createMount(dataDir))
    if (command != null) action.commands = listOf("/bin/sh", "-c", command)
    return action
}