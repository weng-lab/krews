package krews.executor

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.genomics.v2alpha1.Genomics
import com.google.api.services.storage.Storage
import krews.WFile
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private val log = KotlinLogging.logger {}

class GoogleExecutor(workflowConfig: WorkflowConfig) : EnvironmentExecutor {

    private val genomicsClient: Genomics
    private val storageClient: Storage
    private val googleConfig = checkNotNull(workflowConfig.googleExec) { "google-exec config must be present to use Google Executor" }

    init {
        val transport = NetHttpTransport()
        val jsonFactory = JacksonFactory.getDefaultInstance()
        val credentials = GoogleCredential.getApplicationDefault()
        genomicsClient = Genomics.Builder(transport, jsonFactory, credentials).build()
        storageClient = Storage.Builder(transport, jsonFactory, credentials).build()
    }

    override fun prepareDatabaseFile(): String {
        val dbFilePath = Paths.get(googleConfig.localStorageBaseDir, DB_FILENAME).toAbsolutePath()
        val dbStorageObject = "${googleConfig.storageBaseDir}/$DB_FILENAME"
        log.info { "Attempting to download $dbStorageObject from bucket ${googleConfig.storageBucket}..." }
        val fileExists = downloadObject(googleConfig.storageBucket, dbStorageObject, dbFilePath)
        if (fileExists) {
            log.info { "$dbStorageObject not found in bucket ${googleConfig.storageBucket}. A new database file will be used." }
        } else {
            log.info { "$dbStorageObject successfully downloaded to $dbFilePath" }
        }
        return dbFilePath.toString()
    }

    override fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<WFile>) {

    }

    override fun executeTask(workflowRunDir: String, taskConfig: TaskConfig, image: String, script: String?, inputItem: Any, outputItem: Any?) {

    }

    private fun downloadObject(bucket: String, obj: String, downloadPath: Path): Boolean {
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

}