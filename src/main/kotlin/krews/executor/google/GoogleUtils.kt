package krews.executor.google

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.InputStreamContent
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.genomics.v2alpha1.Genomics
import com.google.api.services.genomics.v2alpha1.model.*
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.StorageObject
import krews.config.GoogleWorkflowConfig
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

private val log = KotlinLogging.logger {}

const val APPLICATION_NAME = "krews"
const val STORAGE_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write"
const val LOG_FILE_NAME = "out.txt"
const val DISK_NAME = "disk"

internal val googleClientTransport by lazy { NetHttpTransport() }
internal val googleClientJsonFactory by lazy { JacksonFactory.getDefaultInstance() }
internal val googleClientCredentials by lazy { GoogleCredential.getApplicationDefault() }
internal val googleGenomicsClient by lazy {
    Genomics.Builder(googleClientTransport, googleClientJsonFactory, googleClientCredentials)
        .setApplicationName(APPLICATION_NAME)
        .build()
}
internal val googleStorageClient by lazy {
    Storage.Builder(googleClientTransport, googleClientJsonFactory, googleClientCredentials)
        .setApplicationName(APPLICATION_NAME)
        .build()
}

internal fun createRunPipelineRequest(googleConfig: GoogleWorkflowConfig): RunPipelineRequest {
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

    val actions = mutableListOf<Action>()
    run.pipeline.actions = actions

    return run
}


/**
 * Create a pipeline action that will periodically copy logs to GCS
 *
 * @param frequency: Frequency that logs will be copied into GCS in seconds
 */
internal fun createPeriodicLogsAction(logPath: String, frequency: Int): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    action.commands = listOf("sh", "-c", "while true; do sleep $frequency; gsutil cp /google/logs/output $logPath; done")
    action.flags = listOf("RUN_IN_BACKGROUND")
    return action
}

/**
 * Create a pipeline action that will copy logs to GCS after other actions are complete
 */
internal fun createLogsAction(logPath: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    action.commands = listOf("sh", "-c", "gsutil cp /google/logs/output $logPath")
    action.flags = listOf("ALWAYS_RUN")
    return action
}

/**
 * Create a pipeline action that will download a file from Google Cloud Storage to the Pipelines VM
 */
internal fun createDownloadAction(objectToDownload: String, dataDir: String, file: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    action.commands = listOf("sh", "-c", "gsutil cp $objectToDownload $dataDir/$file; chmod 755 $dataDir/$file")
    action.mounts = listOf(createMount(dataDir))
    return action
}

/**
 * Create a pipeline action that will upload all the contents of the VM's data directory to the given GS bucket
 * if the pipeline has failed
 */
internal fun createDiagnosticUploadAction(diagnosticsGSPath: String, dataDir: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    action.commands = listOf("sh", "-c", "if [[ \"\$GOOGLE_PIPELINE_FAILED\" = \"1\" ]]; then gsutil -m cp -r $dataDir $diagnosticsGSPath; fi")
    action.mounts = listOf(createMount(dataDir))
    action.flags = listOf("ALWAYS_RUN")
    return action
}

/**
 * Create a pipeline action that will upload a file from the Pipelines VM to the specified google storage object.
 */
internal fun createUploadAction(objectToUpload: String, dataDir: String, file: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    // -n flag make sure it does not try to upload objects that already exist
    action.commands = listOf("sh", "-c", "gsutil cp -n $dataDir/$file $objectToUpload")
    action.mounts = listOf(createMount(dataDir))
    return action
}

/**
 * Utility function to create a GCS path for the given GCS bucket and path components
 */
internal fun gcsPath(bucket: String, vararg pathParts: String?): String {
    var gcsPath = "gs://$bucket"
    pathParts.forEach {
        if (it != null) gcsPath += "/$it"
    }
    return gcsPath
}

/**
 * Utility method to create a GCS object path without a bucket / URI for path components
 */
internal fun gcsObjectPath(vararg pathParts: String?): String {
    var gcsObject = ""
    var first = true
    pathParts.forEach {
        if (it != null) {
            if (first) {
                first = false
            } else {
                gcsObject += "/"
            }
            gcsObject += it
        }
    }
    return gcsObject
}

/**
 * Convenience function to create a Mount object for a Pipeline Action with our standard data disk name
 */
internal fun createMount(mountDir: String): Mount {
    val mount = Mount()
    mount.disk = DISK_NAME
    mount.path = mountDir
    return mount
}

/**
 * Copies an object from one location in Google Cloud Storage to Another
 */
internal fun copyObject(googleStorageClient: Storage, fromBucket: String, fromObject: String, toBucket: String, toObject: String) {
    log.info { "Copying Google Cloud Storage object $fromObject in bucket $fromBucket to $toObject in bucket $toBucket..." }
    var rewriteToken: String? = null
    do {
        val fromContent = googleStorageClient.objects().get(fromBucket, fromObject).execute()
        val toContent = StorageObject()
        toContent.updated = fromContent.updated
        val rewrite = googleStorageClient.objects().rewrite(fromBucket, fromObject, toBucket, toObject, toContent)
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
internal fun downloadObject(googleStorageClient: Storage, bucket: String, obj: String, downloadPath: Path): Boolean {
    try {
        log.info { "Downloading file $downloadPath from object $obj in bucket $bucket" }
        val objectInputStream = googleStorageClient.objects().get(bucket, obj).executeMediaAsInputStream()
        Files.copy(objectInputStream, downloadPath, StandardCopyOption.REPLACE_EXISTING)
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
internal fun uploadObject(googleStorageClient: Storage, bucket: String, obj: String, uploadFromPath: Path) {
    log.info { "Uploading file $uploadFromPath to object $obj in bucket $bucket" }
    val contentStream = InputStreamContent(Files.probeContentType(uploadFromPath), Files.newInputStream(uploadFromPath))
    contentStream.length = Files.size(uploadFromPath)
    val objectMetadata = StorageObject().setName(obj)
    googleStorageClient.objects().insert(bucket, objectMetadata, contentStream).execute()
}