package krews.file

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.storage.Storage
import krews.executor.google.APPLICATION_NAME
import krews.executor.google.CLOUD_SDK_IMAGE
import org.joda.time.DateTime

val storageClient: Storage by lazy {
    Storage.Builder(NetHttpTransport(), JacksonFactory.getDefaultInstance(),
        GoogleCredential.getApplicationDefault())
        .setApplicationName(APPLICATION_NAME)
        .build()
}

/**
 * An input file that refers to a file stored in Google Cloud Storage
 * @param bucket: Google Cloud Storage Bucket
 * @param objectPath: GCS object
 * @param path: Unique relative path used by Krews for storage and in task containers. Set to objectPath by default.
 */
class GSInputFile(val bucket: String, val objectPath: String, path: String = objectPath) : InputFile(path) {
    override fun fetchLastModified() = DateTime(storageClient.objects().get(bucket, objectPath).execute().updated.value)
    override fun downloadFileImage() = CLOUD_SDK_IMAGE
    override fun downloadFileCommand(containerBaseDir: String) =
        "gsutil cp gs://$bucket/$objectPath $containerBaseDir/$objectPath"
}