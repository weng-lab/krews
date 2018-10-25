package krews.config


data class GoogleExecConfig (
    val storageBucket: String,
    val storageBaseDir: String?,
    val localStorageBaseDir: String = "."
)

sealed class GoogleCredentials
object ApplicationDefaultGoogleCredentials : GoogleCredentials()
data class ServiceAccountGoogleCredentials (val serviceAccount: String) : GoogleCredentials()
