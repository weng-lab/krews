package krews.config

import jdk.jfr.Frequency


data class GoogleWorkflowConfig (
    val projectId: String,
    val regions: List<String> = listOf(),
    val zones: List<String> = listOf(),
    val storageBucket: String,
    val storageBaseDir: String?,
    val localStorageBaseDir: String = ".",
    // Interval in seconds between checks for pipeline job completion
    val jobCompletionPollInterval: Int = 10,
    // Interval in seconds between pipeline job log uploads to GCS
    val logUploadInterval: Int = 60
)

data class GoogleTaskConfig (
    val machineType: String?,
    val diskSize: Capacity,
    val diskType: GoogleDiskType
)

enum class GoogleDiskType(val value: String) {
    HDD("pd-standard"), SDD("pd-ssd")
}

/*
sealed class GoogleCredentials
object ApplicationDefaultGoogleCredentials : GoogleCredentials()
data class ServiceAccountGoogleCredentials (val serviceAccount: String) : GoogleCredentials()
*/
