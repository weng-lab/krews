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
import krews.TaskDocker
import krews.WFile
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk"
const val DISK_NAME = "data"

class GoogleExecutor(workflowConfig: WorkflowConfig) : EnvironmentExecutor {

    private val genomicsClient: Genomics
    private val storageClient: Storage
    private val googleConfig = checkNotNull(workflowConfig.googleExec) { "google-exec config must be present to use Google Executor" }
    private val dbFilePath = Paths.get(googleConfig.localStorageBaseDir, DB_FILENAME).toAbsolutePath()
    private val dbStorageObject = "${googleConfig.storageBaseDir}/$DB_FILENAME"

    init {
        val transport = NetHttpTransport()
        val jsonFactory = JacksonFactory.getDefaultInstance()
        val credentials = GoogleCredential.getApplicationDefault()
        genomicsClient = Genomics.Builder(transport, jsonFactory, credentials).build()
        storageClient = Storage.Builder(transport, jsonFactory, credentials).build()
    }

    override fun prepareDatabaseFile(): String {
        log.info { "Attempting to download $dbStorageObject from bucket ${googleConfig.storageBucket}..." }
        val fileExists = downloadObject(storageClient, googleConfig.storageBucket, dbStorageObject, dbFilePath)
        if (fileExists) {
            log.info { "$dbStorageObject not found in bucket ${googleConfig.storageBucket}. A new database file will be used." }
        } else {
            log.info { "$dbStorageObject successfully downloaded to $dbFilePath" }
        }
        return dbFilePath.toString()
    }

    override fun pushDatabaseFile() {
        log.info { "Pushing database file $dbFilePath to object $dbStorageObject in bucket ${googleConfig.storageBucket}" }
        uploadObject(storageClient, googleConfig.storageBucket, dbStorageObject, dbFilePath)
    }

    override fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<WFile>) {
        log.info { "Copying cached outputs $outputFiles from workflow run dir $fromWorkflowDir to $toWorkflowDir" }
        outputFiles.forEach { outputFile ->
            val fromObject = "${googleConfig.storageBaseDir}/$RUN_DIR/$fromWorkflowDir/${outputFile.path}"
            val toObject = "${googleConfig.storageBaseDir}/$RUN_DIR/$fromWorkflowDir/${outputFile.path}"
            copyObject(storageClient, googleConfig.storageBucket, fromObject, googleConfig.storageBucket, toObject)
        }

    }

    override fun executeTask(workflowRunDir: String, taskConfig: TaskConfig, taskDocker: TaskDocker,
                             script: String?, inputItem: Any, outputItem: Any?) {
        val run = RunPipelineRequest()
        val pipeline = Pipeline()

        val resources = Resources()
        val virtualMachine = VirtualMachine()
        /*resources.virtualMachine = virtualMachine
        if () {
            virtualMachine.machineType =
        }*/


        val inputFiles = getFilesForObject(inputItem)
        val downloadActions = inputFiles.map { createDownloadAction(googleConfig.storageBucket, googleConfig.storageBaseDir, taskDocker.dataDir, it) }

        genomicsClient.pipelines().run(run)
    }

}

/**
 * Create a pipeline action that will download a file from Google Cloud Storage to the Pipelines VM
 */
fun createDownloadAction(bucket: String, gcsBase: String?, dataDir: String, file: WFile): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE

    var inputFile = "gs://$bucket"
    if (gcsBase != null) {
        inputFile += "/$gcsBase"
    }
    inputFile += "/${file.path}"
    action.commands = listOf("sh", "-c", "gsutil cp $inputFile $dataDir/${file.path}")

    val mount = Mount()
    mount.disk = DISK_NAME
    mount.path = dataDir
    action.mounts = listOf(mount)

    return action
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