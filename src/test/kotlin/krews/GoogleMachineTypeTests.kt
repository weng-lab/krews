package krews

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import krews.config.GoogleMachineClass
import krews.config.GoogleTaskConfig
import krews.config.googleMachineType
import krews.core.GB

class GoogleMachineTypeTests : StringSpec({
    "googleMachineType should return machineType if set in google task config" {
        val googleConfig = GoogleTaskConfig(
            machineType = "test-machine-type",
            machineClass = GoogleMachineClass.STANDARD,
            cpus = 10,
            mem = 100.GB,
            memPerCpu = 10.GB
        )
        googleMachineType(googleConfig, 15, 150.GB) shouldBe "test-machine-type"
    }

    "googleMachineType should return a default when given nothing else to work with" {
        googleMachineType(GoogleTaskConfig(), null, null) shouldBe "n1-standard-1"
    }

    "googleMachineType should return a standard class machine with enough cpu" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.STANDARD
        )
        googleMachineType(googleConfig, 15, 50.GB) shouldBe "n1-standard-16"
    }

    "googleMachineType should return a standard class machine with enough memory" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.STANDARD
        )
        googleMachineType(googleConfig, 15, 100.GB) shouldBe "n1-standard-32"
    }

    "googleMachineType should return the largest possible machine when none in the class can handle the requirements" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.STANDARD
        )
        googleMachineType(googleConfig, 100, null) shouldBe "n1-standard-96"
    }

    "googleMachineType should maintain memPerCpu for custom machines" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 2.GB
        )
        googleMachineType(googleConfig, 6, null) shouldBe "custom-6-12288"
    }

    "googleMachineType should only allow even numbers for custom machines" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 2.GB
        )
        googleMachineType(googleConfig, 5, null) shouldBe "custom-6-12288"
    }

    "googleMachineType should not go below the minimum memPerCpu of .9GB" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 0.5.GB
        )
        googleMachineType(googleConfig, 1, null) shouldBe "custom-1-922"
    }

    "googleMachineType should not go above the maximum memPerCpu of 6.5GB" {
        val googleConfig = GoogleTaskConfig(
            machineClass = GoogleMachineClass.CUSTOM,
            memPerCpu = 7.GB
        )
        googleMachineType(googleConfig, 1, null) shouldBe "custom-1-6656"
    }
})