package krews

import krews.config.*
import krews.core.*
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*

class GoogleMachineTypeTests {

    @Test fun `googleMachineType should return machineType if set in google task config`() {
        val googleConfig = GoogleTaskConfig(
            machineType = "test-machine-type",
            machineClass = GoogleMachineClass.STANDARD,
            cpus = 10,
            mem = 100.GB,
            memPerCpu = 10.GB
        )
        assertThat(googleMachineType(googleConfig, 15, 150.GB)).isEqualTo("test-machine-type")
    }

    @Test fun `googleMachineType should return a default when given nothing else to work with`() {
        assertThat(googleMachineType(GoogleTaskConfig(), null, null)).isEqualTo("n1-standard-1")
    }

    @Test fun `googleMachineType should return a standard class machine with enough cpu`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.STANDARD
        )
        assertThat(googleMachineType(googleConfig, 15, 50.GB)).isEqualTo("n1-standard-16")
    }

    @Test fun `googleMachineType should return a standard class machine with enough memory`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.STANDARD
        )
        assertThat(googleMachineType(googleConfig, 15, 100.GB)).isEqualTo("n1-standard-32")
    }

    @Test fun `googleMachineType should return the largest possible machine when none in the class can handle the requirements`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.STANDARD
        )
        assertThat(googleMachineType(googleConfig, 100, null)).isEqualTo("n1-standard-96")
    }

    @Test fun `googleMachineType should maintain memPerCpu for custom machines`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 2.GB
        )
        assertThat(googleMachineType(googleConfig, 6, null)).isEqualTo("custom-6-12288")
    }

    @Test fun `googleMachineType should only allow even numbers for custom machines`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 2.GB
        )
        assertThat(googleMachineType(googleConfig, 5, null)).isEqualTo("custom-6-12288")
    }

    @Test fun `googleMachineType should not go below the minimum memPerCpu of _9GB`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 0.5.GB
        )
        assertThat(googleMachineType(googleConfig, 1, null)).isEqualTo("custom-1-922")
    }

    @Test fun `googleMachineType should not go above the maximum memPerCpu of 6_5GB`() {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 7.GB
        )
        assertThat(googleMachineType(googleConfig, 1, null)).isEqualTo("custom-1-6656")
    }
}