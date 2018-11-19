package krews.file

import krews.executor.google.CLOUD_SDK_IMAGE
import krews.executor.google.googleStorageClient

/**
 * An input file that refers to a file stored in Google Cloud Storage
 * @param bucket: Google Cloud Storage Bucket
 * @param objectPath: GCS object
 * @param path: Unique relative path used by Krews for storage and in task containers. Set to objectPath by default.
 */
class GSInputFile(val bucket: String, val objectPath: String, path: String = objectPath) : InputFile(path) {
    override fun fetchLastModified() = googleStorageClient.objects().get(bucket, objectPath).execute().updated.value
    override fun downloadFileImage() = CLOUD_SDK_IMAGE
    override fun downloadFileCommand(containerBaseDir: String) =
        "gsutil cp gs://$bucket/$objectPath $containerBaseDir/$path"

    override fun toString() = "GSInputFile(bucket='$bucket', objectPath='$objectPath', path='$path')"
}