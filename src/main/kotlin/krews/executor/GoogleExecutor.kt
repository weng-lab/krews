package krews.executor

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.InputStreamContent
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.genomics.v2alpha1.Genomics
import com.google.api.services.genomics.v2alpha1.model.*
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.StorageObject
import krews.WFile
import krews.config.CapacityType
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"
const val DISK_NAME = "data"
const val DEFAULT_LOG_UPLOAD_FREQ = 60

class GoogleExecutor(workflowConfig: WorkflowConfig) : EnvironmentExecutor {

    private val genomicsClient: Genomics
    private val storageClient: Storage
    private val googleConfig = checkNotNull(workflowConfig.google) { "google workflow config must be present to use Google Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir
    private val dbFilePath = Paths.get(googleConfig.localStorageBaseDir, DB_FILENAME).toAbsolutePath()
    private val dbStorageObject = "$gcsBase/$DB_FILENAME"

    init {
        val transport = NetHttpTransport()
        val jsonFactory = JacksonFactory.getDefaultInstance()
        val credentials = GoogleCredential.getApplicationDefault()
        genomicsClient = Genomics.Builder(transport, jsonFactory, credentials).build()
        storageClient = Storage.Builder(transport, jsonFactory, credentials).build()
    }

    override fun prepareDatabaseFile(): String {
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
            val fromObject = "$gcsBase/$RUN_DIR/$fromWorkflowDir/${outputFile.path}"
            val toObject = "$gcsBase/$RUN_DIR/$fromWorkflowDir/${outputFile.path}"
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

        val virtualMachine = VirtualMachine()
        resources.virtualMachine = virtualMachine
        if (taskConfig.google?.machineType != null) {
            virtualMachine.machineType = taskConfig.google.machineType
        }

        val disk = Disk()
        virtualMachine.disks = listOf(disk)
        disk.name = DISK_NAME
        if (taskConfig.google?.diskSize != null) {
            disk.sizeGb = taskConfig.google.diskSize.toType(CapacityType.GB).toInt()
        }

        val actions = mutableListOf<Action>()
        pipeline.actions = actions

        // Create actions to download each input file
        val inputFiles = getFilesForObject(inputItem)
        val downloadActions = inputFiles.map { createDownloadAction(bucket, gcsBase, dockerDataDir, it) }
        actions.addAll(downloadActions)

        // Create the action that runs the task
        val executeAction = createExecuteAction(dockerImage, command)
        actions.add(executeAction)

        // Create the actions to upload each output file
        val outputFiles = getFilesForObject(outputItem)
        val uploadActions = outputFiles.map { createUploadAction(bucket, gcsBase, dockerDataDir, it) }
        actions.addAll(uploadActions)

        // Create action to periodically copy logs to GCS
        val logCopyFrequency = taskConfig.google?.logUploadFrequency ?: DEFAULT_LOG_UPLOAD_FREQ
        val logsAction = createLogsAction(bucket, gcsBase, taskRunId, logCopyFrequency)
        actions.add(logsAction)

        genomicsClient.pipelines().run(run)
    }

}

/**
 * Create a pipeline action that will execute the task
 */
fun createExecuteAction(dockerImage: String, command: String?): Action {
    val action = Action()
    action.imageUri = dockerImage
    if (command != null) action.commands = listOf("/bin/sh", "-c", command)
    return action
}

/**
 * Create a pipeline action that will periodically copy logs to GCS
 *
 * @param frequency: Frequency that logs will be copied into GCS in seconds
 */
fun createLogsAction(bucket: String, gcsBase: String?, taskRunId: Int, frequency: Int): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE

    val logPath = gcsPath(bucket, gcsBase, taskRunId.toString())
    action.commands = listOf("sh", "-c", "while true; sleep $frequency; gsutil cp /google/logs/output $logPath; done")
    action.flags = listOf("RUN_IN_BACKGROUND")
    return action
}

/**
 * Create a pipeline action that will download a file from Google Cloud Storage to the Pipelines VM
 */
fun createDownloadAction(bucket: String, gcsBase: String?, dataDir: String, file: WFile): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE

    val inputFile = gcsPath(bucket, gcsBase, file.path)
    action.commands = listOf("sh", "-c", "gsutil cp $inputFile $dataDir/${file.path}")
    action.mounts = listOf(createDataDirMount(dataDir))
    return action
}

/**
 * Create a pipeline action that will upload a file from the Pipelines VM to the
 */
fun createUploadAction(bucket: String, gcsBase: String?, dataDir: String, file: WFile): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE

    val outputFile = gcsPath(bucket, gcsBase, file.path)
    action.commands = listOf("sh", "-c", "gsutil cp $dataDir/${file.path} $outputFile")
    action.mounts = listOf(createDataDirMount(dataDir))
    action.flags = listOf("ALWAYS_RUN")
    return action
}

/**
 * Create a GCS path for the given GCS bucket, base directory, and path
 */
fun gcsPath(bucket: String, gcsBase: String?, path: String): String {
    var gcsFile = "gs://$bucket"
    if (gcsBase != null) {
        gcsFile += "/$gcsBase"
    }
    gcsFile += "/$path"
    return gcsFile
}

/**
 * Convenience function to create a Mount object for a Pipeline Action with our standard data disk name
 */
fun createDataDirMount(dataDir: String): Mount {
    val mount = Mount()
    mount.disk = DISK_NAME
    mount.path = dataDir
    return mount
}

/**
 * Copies an object from one location in Google Cloud Storage to Another
 */
private fun copyObject(storageClient: Storage, fromBucket: String, fromObject: String, toBucket: String, toObject: String) {
    log.info { "Copying Google Cloud Storage object $fromObject in bucket $fromBucket to $toObject in bucket $toBucket..." }
    var rewriteToken: String? = null
    do {
        val rewrite = storageClient.objects().rewrite(fromBucket, fromObject, toBucket, toObject, null)
        rewrite.rewriteToken = rewriteToken
        val rewriteResponse = rewrite.execute()
        rewriteToken = rewriteResponse.rewriteToken
        if (!rewriteResponse.done) {
            log.info { "${rewriteResponse.totalBytesRewritten} bytes copied" }
        }
    } while (!rewriteResponse.done)
    log.info { "Copy complete!" }
}

/**
 * Downloads an "object" from Google Cloud Storage to local file system
 */
private fun downloadObject(storageClient: Storage, bucket: String, obj: String, downloadPath: Path): Boolean {
    try {
        val objectInputStream = storageClient.objects().get(bucket, obj).executeMediaAsInputStream()
        Files.copy(objectInputStream, downloadPath)
        return true
    } catch (e: GoogleJsonResponseException) {
        if (e.statusCode == 404) {
            return false
        }
        throw e
    }
}

/**
 * Uploads a file from a local file system to Google Cloud Storage
 */
private fun uploadObject(storageClient: Storage, bucket: String, obj: String, uploadFromPath: Path) {
    val contentStream = InputStreamContent(Files.probeContentType(uploadFromPath), Files.newInputStream(uploadFromPath))
    contentStream.length = Files.size(uploadFromPath)
    val objectMetadata = StorageObject().setName(obj)
    storageClient.objects().insert(bucket, objectMetadata, contentStream).execute()
}