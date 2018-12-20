package krews

import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.typesafe.config.ConfigFactory
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import krews.config.*
import krews.core.Capacity
import krews.core.CapacityType
import krews.core.workflow
import krews.file.File
import krews.file.InputFile
import krews.file.LocalInputFile
import krews.misc.CacheView
import krews.misc.mapper
import reactor.core.publisher.toMono

private data class TestWorkflowParams(
    val withDefault: String = "default",
    val withoutDefault: String,
    val nullableFile: File?,
    val complex: ComplexType?,
    val map: Map<String, String>?,
    val list: List<String>?
)

private data class ComplexType(
    val intValue: Int,
    val doubleValue: Double
)

private var parsedParams: TestWorkflowParams? = null

private fun configSampleWorkflow() = workflow("config-sample") {
    parsedParams = params<TestWorkflowParams>()

    task<String, String>("sample", "".toMono()) {
        dockerImage = "test"
        output = ""
        command = ""
    }

    task<String, String>("sample2", "".toMono(), "small") {
        dockerImage = "test"
        output = ""
        command = ""
    }

    task<String, String>("sample3", "".toMono(), "large") {
        dockerImage = "test"
        output = ""
        command = ""
    }
}

private val brokenParamsConfig =
        """
        params = {
            with-default = "with-test"
        }
        """.trimIndent()

private val sparseParamsConfig =
        """
        params = {
            without-default = "without-test"
        }
        """.trimIndent()
private val parsedSparseParams = TestWorkflowParams(
    withDefault = "default",
    withoutDefault = "without-test",
    nullableFile = null,
    complex = null,
    map = null,
    list = null
)

private val completeParamsConfig =
        """
        params {
            with-default = test1
            without-default = test2
            nullable-file {
                "-type" = "krews.file.LocalInputFile"
                local-path = "path/to/file.txt"
                path = "file.txt"
            }
            complex {
                int-value = 1
                double-value = 2
            }
            map {
                text = test-text
            }
            list = ["list-test"]
        }
        """.trimIndent()
private val parsedCompleteParams = TestWorkflowParams(
    withDefault = "test1",
    withoutDefault = "test2",
    nullableFile = LocalInputFile("path/to/file.txt", "file.txt"),
    complex = ComplexType(
        intValue = 1,
        doubleValue = 2.0
    ),
    map = mapOf("text" to "test-text"),
    list = listOf("list-test")
)

private val completeTestConfig =
        """
        $completeParamsConfig

        parallelism = 5

        google = {
            project-id = test-project
            storage-bucket = test-bucket
        }

        task.default {
            params {
                my-shared-thing = someval
            }

            google {
                machine-type = n1-standard-2
                disk-size = 10GB
            }
        }

        # Overrides for gzip krews.core.task
        task.small {
            google {
                machine-type = n1-standard-1
                disk-size = 5GB
            }
        }

        # Overrides for gzip krews.core.task
        task.sample {
            params {
                my-shared-thing = override
                db-url = "postgresql://somewhere:5432/mydb"
                db-user = admin
                db-password = "Password123!"
            }
            google {
                disk-size = 30GB
            }
        }
        """.trimIndent()

class ConfigTests : StringSpec({
    "Parsing params without missing not-nullable field should throw exception" {
        val config = ConfigFactory.parseString(brokenParamsConfig)
        val params = createParamsForConfig(config)
        shouldThrow<MissingKotlinParameterException> {
            configSampleWorkflow().build(params)
        }
    }

    "Parsing workflow params with defaults and nullability should work" {
        val config = ConfigFactory.parseString(sparseParamsConfig)
        val params = createParamsForConfig(config)
        configSampleWorkflow().build(params)
        parsedParams shouldBe parsedSparseParams
    }

    "Parsing workflow params with all fields should work" {
        val config = ConfigFactory.parseString(completeParamsConfig)
        val params = createParamsForConfig(config)
        configSampleWorkflow().build(params)
        parsedParams!!.withDefault shouldBe parsedCompleteParams.withDefault
        parsedParams!!.withoutDefault shouldBe parsedCompleteParams.withoutDefault
        parsedParams!!.complex shouldBe parsedCompleteParams.complex
        parsedParams!!.nullableFile!!.path shouldBe parsedCompleteParams.nullableFile!!.path
        parsedParams!!.nullableFile!!.shouldBeInstanceOf<InputFile>()
        parsedParams!!.list shouldBe parsedCompleteParams.list
        parsedParams!!.map shouldBe parsedCompleteParams.map
    }

    "createWorkflowConfig should create complete complex task-based configurations" {
        val config = ConfigFactory.parseString(completeTestConfig)
        val params = createParamsForConfig(config)
        val workflow = configSampleWorkflow().build(params)
        val workflowConfig = createWorkflowConfig(config, workflow)
        workflowConfig.google shouldBe GoogleWorkflowConfig(
            projectId = "test-project",
            storageBucket = "test-bucket"
        )
        workflowConfig.local shouldBe null
        workflowConfig.tasks["sample"] shouldBe TaskConfig(
            params = mapOf(
                "my-shared-thing" to "override",
                "db-url" to "postgresql://somewhere:5432/mydb",
                "db-user" to "admin",
                "db-password" to "Password123!"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-2",
                diskSize = Capacity(30, CapacityType.GB)
            )
        )

        workflowConfig.tasks["sample2"] shouldBe TaskConfig(
            params = mapOf(
                "my-shared-thing" to "someval"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-1",
                diskSize = Capacity(5, CapacityType.GB)
            )
        )

        workflowConfig.tasks["sample3"] shouldBe TaskConfig(
            params = mapOf(
                "my-shared-thing" to "someval"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-2",
                diskSize = Capacity(10, CapacityType.GB)
            )
        )
    }

    "Json writer with CacheView should not write InputFile.cache" {
        val json = mapper
            .writerWithView(CacheView::class.java)
            .forType(File::class.java)
            .writeValueAsString(LocalInputFile("local/path", "path", true))
        json shouldBe """{"-type":"krews.file.LocalInputFile","local-path":"local/path","path":"path","last-modified":-1}"""
    }

})