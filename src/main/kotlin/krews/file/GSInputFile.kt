package krews.file

import krews.executor.google.CLOUD_SDK_IMAGE
import krews.executor.google.downloadObject
import krews.executor.google.googleStorageClient
import java.nio.file.Path
import java.nio.file.Paths

/**
 * An input file that refers to a file stored in Google Cloud Storage
 * @param bucket: Google Cloud Storage Bucket
 * @param objectPath: GCS object
 * @param path: Unique relative path used by Krews for storage and in task containers. Set to objectPath by default.
 */
data class GSInputFile(val bucket: String, val objectPath: String, override val path: String = objectPath) : InputFile() {
    override fun downloadFileImage() = CLOUD_SDK_IMAGE
    override fun downloadFileCommand(containerBaseDir: String) =
        "gsutil cp gs://$bucket/$objectPath $containerBaseDir/$path"
}

private val gsPathRegex = """gs://(.*?)/(.*)""".toRegex()

fun parseGSURL(gsURL: String): ParsedGSUrl {
    val results = gsPathRegex.find(gsURL)!!
    val bucket = results.groups[1]!!.value
    val objectPath = results.groups[2]!!.value
    val fileName = objectPath.split("/").last()
    return ParsedGSUrl(bucket, objectPath, fileName)
}
data class ParsedGSUrl(val bucket: String, val objectPath: String, val fileName: String)