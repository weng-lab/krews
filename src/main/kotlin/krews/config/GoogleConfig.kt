package krews.config

import krews.core.*
import kotlin.math.roundToInt


data class GoogleWorkflowConfig(
    val projectId: String,
    // Required region where your life sciences where operation metadata is stored
    // Default for backwards compatibility. It's currently the only supported location anyway.
    val lifeSciencesLocation: String = "us-central1",
    val regions: List<String> = listOf(),
    val zones: List<String> = listOf(),
    val bucket: String,
    // Interval in seconds between checks for pipeline job completion
    val jobCompletionPollInterval: Int = 10,
    // Interval in seconds between pipeline job log uploads to GCS
    val logUploadInterval: Int = 60
)

data class GoogleTaskConfig(
    // A native google compute engine machine type. If this is set, it's always used regardless of other configs.
    val machineType: String? = null,
    // A class of machine. Useful for when you don't know the needed resources until runtime.
    val machineClass: GoogleMachineClass? = null,
    // Number of cpus. Can be used to override the runtime value.
    val cpus: Int? = null,
    // Memory capacity. Can be used to override the runtime value.
    val mem: Capacity? = null,
    // An optional memory per cpu ratio used when you don't have both fields available and don't want to use a machine class.
    val memPerCpu: Capacity? = null,
    // Disk Size. Can be used to override the runtime value.
    val diskSize: Capacity? = null,
    // Type of disk, HDD vs SSD.
    val diskType: GoogleDiskType = GoogleDiskType.HDD,
    // GPUs to attach to the VM
    val gpus: GoogleGPUConfig? = null,
    // Image for the machine boot disk
    val bootImage: String? = null,
    // Size of the boot disk
    val bootDiskSizeGb: Int? = null
)

data class GoogleGPUConfig(
    // The type of GPU to attach
    val gpuType: String,
    // The number of GPUs to attach
    val gpuCount: Long,
    // Boot image to use
    val bootImage: String? = null
)

enum class GoogleDiskType(val value: String) {
    HDD("pd-standard"), SSD("pd-ssd")
}

private class MachineSpecs(val cpus: Int, val mem: Capacity)

private fun specs(cpus: Int, mem: Capacity) = MachineSpecs(cpus, mem)

enum class GoogleMachineClass(internal val prefix: String, private val availableSpecs: List<MachineSpecs>? = null) {
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
        override fun machineType(cpus: Int?, mem: Capacity?) =
            throw Exception("machineType should not be called for GoogleMachineClass.CUSTOM")
    };

    private fun cpusToMachineType(cpus: Int) = "$prefix-$cpus"

    open fun machineType(cpus: Int?, mem: Capacity?): String {
        // If we're calling this function, assume availableSpecs is not null
        val availableSpecs = availableSpecs!!

        // Find the specs required to fulfill the given cpus requirement
        val specsNeededForCpus = if (cpus == null) availableSpecs[0] else availableSpecs.firstOrNull { cpus <= it.cpus }?: availableSpecs.last()

        // Find the specs required to fulfill the given memory requirement
        val specsNeededForMem = if (mem == null) availableSpecs[0] else availableSpecs.firstOrNull { mem.inB() <= it.mem.inB() }?: availableSpecs.last()

        return cpusToMachineType(Math.max(specsNeededForCpus.cpus, specsNeededForMem.cpus))
    }
}

private val minMemPerCpu = 0.9.GB
private val maxMemPerCpu = 6.5.GB

fun googleMachineType(googleConfig: GoogleTaskConfig?, runtimeCpus: Int?, runtimeMem: Capacity?): String {
    if (googleConfig?.machineType != null) return googleConfig.machineType

    var cpus = googleConfig?.cpus ?: runtimeCpus
    var mem = googleConfig?.mem ?: runtimeMem
    var memPerCpu = googleConfig?.memPerCpu

    val machineClass = googleConfig?.machineClass
        ?: if (memPerCpu != null) GoogleMachineClass.CUSTOM else GoogleMachineClass.STANDARD

    if (machineClass != GoogleMachineClass.CUSTOM) {
        return machineClass.machineType(cpus, mem)
    }

    if (cpus == null && mem == null) {
        return GoogleMachineClass.STANDARD.machineType(null, null)
    }

    // CPUs can only be 1 or even numbers
    if (cpus != null && cpus > 1 && cpus % 2 == 1) cpus++

    if (cpus == null || mem == null) {
        // We need to fill in either cpus or memory
        if (memPerCpu == null) {
            // We weren't given a cpuToMem ratio, so just use a cheaper standard with the default ratio.
            return GoogleMachineClass.STANDARD.machineType(cpus, mem)
        } else {
            // We were given a memPerCpu ratio, so we will compute the missing attribute

            // If the given memPerCpu is lower than minimum, set to minimum
            if (memPerCpu.inB() < minMemPerCpu.inB()) memPerCpu = minMemPerCpu
            // If the given memPerCpu is greater than maximum, set to maximum
            if (memPerCpu.inB() > maxMemPerCpu.inB()) memPerCpu = maxMemPerCpu

            if (cpus == null) {
                cpus = (mem!!.inB() / memPerCpu.inB()).toInt()
                if (cpus > 1 && cpus % 2 == 1) cpus++
            }
            if (mem == null) mem = (cpus * memPerCpu.inB()).B
        }
    }

    // Do a final cpu to memory ratio check, adjusting memory up or down as needed
    val computedMemPerCpu = (mem.inB().toDouble() / cpus).B
    if (computedMemPerCpu.inB() < minMemPerCpu.inB()) mem = (cpus * minMemPerCpu.inB()).B
    if (computedMemPerCpu.inB() > maxMemPerCpu.inB()) mem = (cpus * minMemPerCpu.inB()).B

    return "${machineClass.prefix}-$cpus-${mem.inMB().roundToInt()}"
}