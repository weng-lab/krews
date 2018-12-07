
package krews.config


data class GoogleWorkflowConfig (
    val projectId: String,
    val regions: List<String> = listOf(),
    val zones: List<String> = listOf(),
    val storageBucket: String,
    val storageBaseDir: String? = null,
    // Interval in seconds between checks for pipeline job completion
    val jobCompletionPollInterval: Int = 10,
    // Interval in seconds between pipeline job log uploads to GCS
    val logUploadInterval: Int = 60
)

data class GoogleTaskConfig (
    val machineType: String? = null,
    val diskSize: Capacity = Capacity(500, CapacityType.GB),
    val diskType: GoogleDiskType = GoogleDiskType.HDD
)

enum class GoogleDiskType(val value: String) {
    HDD("pd-standard"), SSD("pd-ssd")
}

/*
sealed class GoogleCredentials
object ApplicationDefaultGoogleCredentials : GoogleCredentials()
data class ServiceAccountGoogleCredentials (val serviceAccount: String) : GoogleCredentials()
*/
