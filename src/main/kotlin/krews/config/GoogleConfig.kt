package krews.config

import krews.core.Capacity
import krews.core.CapacityType
import krews.core.GB
import krews.core.inGB


data class GoogleWorkflowConfig(
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

data class GoogleTaskConfig(
    val machineType: String? = null,
    val diskSize: Capacity = 500.GB,
    val diskType: GoogleDiskType = GoogleDiskType.HDD
)

enum class GoogleDiskType(val value: String) {
    HDD("pd-standard"), SSD("pd-ssd")
}

private class MachineSpecs(val cpus: Int, val mem: Capacity)

private fun specs(cpus: Int, mem: Capacity) = MachineSpecs(cpus, mem)

enum class GoogleMachineClass(protected val prefix: String, protected val availableSpecs: List<MachineSpecs>? = null) {
    STANDARD(
        "n1-standard",
        listOf(
            specs(1, 3.75.GB),
            specs(2, 7.5.GB),
            specs(4, 15.GB),
            specs(8, 50.GB),
            specs(16, 60.GB),
            specs(32, 120.GB),
            specs(64, 240.GB),
            specs(96, 360.GB)
        )
    ),
    HIGHMEM(
        "n1-highmem",
        listOf(
            specs(2, 13.GB),
            specs(4, 26.GB),
            specs(8, 52.GB),
            specs(16, 104.GB),
            specs(32, 208.GB),
            specs(64, 416.GB),
            specs(96, 624.GB)
        )
    ),
    HIGHCPU(
        "n1-highcpu",
        listOf(
            specs(2, 1.8.GB),
            specs(4, 3.6.GB),
            specs(8, 7.2.GB),
            specs(16, 14.4.GB),
            specs(32, 28.8.GB),
            specs(64, 57.6.GB),
            specs(96, 86.4.GB)
        )
    ),
    ULTRAMEM(
        "n1-ultramem",
        listOf(
            specs(40, 961.GB),
            specs(80, 1922.GB),
            specs(160, 3844.GB)
        )
    ),
    CUSTOM("custom") {
        val minMemPerCpu = 0.9.GB
        val maxMemPerCpu = 6.5.GB
        val defaultMemPerCpu = 3.75.GB

        fun customMachineType(cpus: Int?, mem: Capacity?, memPerCpu: Capacity?) {
            if () {

            }
        }

        override fun machineType(cpus: Int?, mem: Capacity?): String {
            if (cpus == null || mem == null) {
                return STANDARD.machineType(cpus, mem)
            }
            var cpusUsed = checkNotNull(cpus) { "CPUs must be set to use custom machine class" }
            var memUsed = checkNotNull(mem) { "Memory must be set to use custom machine class" }
            // CPUs can only be 1 or even numbers
            if (cpusUsed > 1 && cpusUsed % 2 == 1) {
                cpusUsed++
            }
            // If cpu-to-mem ratio too low
            if (cpusUsed * minMemPerCpu.inGB() > memUsed.inGB()) {

            }
            return "$prefix-$cpusUsed-$memUsed"
        }
    };

    open fun machineType(cpus: Int?, mem: Capacity?): String {
        TODO()
    }
}
