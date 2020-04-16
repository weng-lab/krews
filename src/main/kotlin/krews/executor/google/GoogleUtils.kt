package krews.executor.google

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.InputStreamContent
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.lifesciences.v2beta.CloudLifeSciences
import com.google.api.services.lifesciences.v2beta.model.*
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.StorageObject
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import krews.config.GoogleWorkflowConfig
import krews.file.NONE_SUFFIX
import mu.KotlinLogging
import java.nio.file.*

private val log = KotlinLogging.logger {}

const val APPLICATION_NAME = "krews"
const val STORAGE_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write"
const val LOG_FILE_NAME = "out.txt"
const val DISK_NAME = "disk"

internal val googleClientTransport by lazy { NetHttpTransport() }
internal val googleClientJsonFactory by lazy { JacksonFactory.getDefaultInstance() }
internal val googleLifeSciencesClient by lazy {
    val credentials = HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/cloud-platform"))
    CloudLifeSciences.Builder(googleClientTransport, googleClientJsonFactory, credentials)
        .setApplicationName(APPLICATION_NAME)
        .build()
}
internal val googleStorageClient by lazy {
    val credentials = HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()
        .createScoped("https://www.googleapis.com/auth/cloud-platform"))
    Storage.Builder(googleClientTransport, googleClientJsonFactory, credentials)
        .setApplicationName(APPLICATION_NAME)
        .build()
}

internal fun createRunPipelineRequest(googleConfig: GoogleWorkflowConfig): RunPipelineRequest {
    val run = RunPipelineRequest()
    val pipeline = Pipeline()
    run.pipeline = pipeline

    val resources = Resources()
    pipeline.resources = resources
    if (googleConfig.zones.isNotEmpty()) {
        resources.zones = googleConfig.zones
    } else if (googleConfig.regions.isNotEmpty()) {
        resources.regions = googleConfig.regions
    }

    val actions = mutableListOf<Action>()
    run.pipeline.actions = actions

    return run
}

private fun shellRetry(command: String) =
    "for i in \$(seq 1 5); do [ \$i -gt 1 ] && sleep 5; $command && s=0 && break || s=\$?; done; (exit \$s)"

/**
 * Create a pipeline action that will periodically copy logs to GCS or fail silently
 *
 * @param frequency: Frequency that logs will be copied into GCS in seconds
 */
internal fun createPeriodicLogsAction(logPath: String, frequency: Int): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    action.commands = listOf("sh", "-c", "while true; do sleep $frequency; gsutil -q cp /google/logs/output $logPath || true; done")
    action.runInBackground = true
    return action
}

/**
 * Create a pipeline action that will copy logs to GCS after other actions are complete
 */
internal fun createLogsAction(logPath: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    action.commands = listOf("sh", "-c", shellRetry("gsutil -q cp /google/logs/output $logPath"))
    action.alwaysRun = true
    return action
}

/**
 * Create a pipeline action that will download a file from Google Cloud Storage to the Pipelines VM
 */
internal fun createDownloadAction(objectToDownload: String, dataDir: String, file: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    val copyCmd = shellRetry("gsutil -q cp $objectToDownload $dataDir/$file")
    action.commands = listOf("sh", "-c", "set -x; $copyCmd; chmod 755 $dataDir/$file")
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
    val copyCmd = shellRetry("gsutil -m cp -r $dataDir $diagnosticsGSPath")
    action.commands = listOf("sh", "-c", "if [[ \"\$GOOGLE_PIPELINE_FAILED\" = \"1\" ]]; then $copyCmd; fi")
    action.mounts = listOf(createMount(dataDir))
    action.alwaysRun = true
    return action
}

/**
 * Create a pipeline action that will upload a file from the Pipelines VM to the specified google storage object.
 */
internal fun createUploadAction(objectToUpload: String, dataDir: String, file: String, optional: Boolean): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    val filePath = "$dataDir/$file"
    // If it's present upload the file
    // If it's missing (and optional) upload an empty file with suffix indicating it wasn't created
    val presentCopyCmd = "gsutil -q cp $filePath $objectToUpload"
    val copyCmd = if (optional) {
        val missingCopyCmd = "echo \"\" | gsutil -q cp - $objectToUpload.$NONE_SUFFIX"
        "if [ -f $filePath ]; then $presentCopyCmd; else $missingCopyCmd; fi"
    } else presentCopyCmd
    val copyCmdWithRetry = shellRetry(copyCmd)
    action.commands = listOf("sh", "-c", "set -x; $copyCmdWithRetry")
    action.mounts = listOf(createMount(dataDir))
    return action
}

/**
 * Create a pipeline action that will upload a directory from the Pipelines VM to the specified google storage object.
 */
internal fun createUploadDirectoryAction(objectToUpload: String, dataDir: String, file: String): Action {
    val action = Action()
    action.imageUri = CLOUD_SDK_IMAGE
    val filePath = "$dataDir/$file"
    val copyCmd = "gsutil -q cp -r $filePath $objectToUpload"
    val copyCmdWithRetry = shellRetry(copyCmd)
    action.commands = listOf("sh", "-c", "set -x; $copyCmdWithRetry")
    action.mounts = listOf(createMount(dataDir))
    return action
}

/**
 * Utility function to create a GCS path for the given GCS bucket and path components
 */
internal fun gcsPath(bucket: String, vararg pathParts: String?): String {
    var gcsPath = "gs://$bucket"
    pathParts.forEach {
        if (it != null) gcsPath += if (it.startsWith("/")) it else "/$it"
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

/**
 * Returns a list of files in a given directory. This is recursive.
 */
internal fun listFiles(googleStorageClient: Storage, bucket: String, dir: String): List<String> {
    val fullDir = if (dir.endsWith("/")) {
        dir
    } else {
        dir + "/"
    }
    val list = googleStorageClient
        .objects()
        .list(bucket)
        .setPrefix(fullDir)
        .execute()
    return list.getItems().map { it.name.replace(fullDir, "") }
}