package krews.config


data class GoogleWorkflowConfig (
    val storageBucket: String,
    val storageBaseDir: String?,
    val localStorageBaseDir: String = "."
)

data class GoogleTaskConfig (
    val machineType: String?,
    val diskSize: Capacity,
    val diskType: GoogleDiskType,
    // Frequency at which task logs are copied to GCS in seconds
    val logUploadFrequency: Int
)

enum class GoogleDiskType(val value: String) {
    HDD("pd-standard"), SDD("pd-ssd")
}

/*
sealed class GoogleCredentials
object ApplicationDefaultGoogleCredentials : GoogleCredentials()
data class ServiceAccountGoogleCredentials (val serviceAccount: String) : GoogleCredentials()
*/
