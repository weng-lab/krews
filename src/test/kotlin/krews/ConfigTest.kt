package krews

import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.typesafe.config.ConfigFactory
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import krews.config.*
import krews.core.*
import reactor.core.publisher.toMono
import java.lang.IllegalArgumentException

private data class TestWorkflowParams(
    val withDefault: String = "default",
    val withoutDefault: String,
    val nullable: String?,
    val complex: ComplexType?
)

private data class ComplexType(
    val intValue: Int,
    val doubleValue: Double
)

private data class TestTaskParams(
    val value: String
)

private var parsedParams: TestWorkflowParams? = null

private fun configSampleWorkflow() = workflow("config-sample") {
    parsedParams = params<TestWorkflowParams>()

    task<String, String>("sample") {
        dockerImage = "test"
        input = "".toMono()
        outputFn { "" }
        commandFn { "" }
    }

    task<String, String>("sample2") {
        labels = listOf("small")
        dockerImage = "test"
        input = "".toMono()
        outputFn { "" }
        commandFn { "" }
    }

    task<String, String>("sample3") {
        labels = listOf("large")
        dockerImage = "test"
        input = "".toMono()
        outputFn { "" }
        commandFn { "" }
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
    nullable = null,
    complex = null
)

private val completeParamsConfig =
        """
        params = {
            with-default = test1
            without-default = test2
            nullable = test3
            complex = {
                int-value = 1
                double-value = 2
            }
        }
        """.trimIndent()
private val parsedCompleteParams = TestWorkflowParams(
    withDefault = "test1",
    withoutDefault = "test2",
    nullable = "test3",
    complex = ComplexType(
        intValue = 1,
        doubleValue = 2.0
    )
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
            env {
                MY_SHARED_THING = someval
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
            env {
                MY_SHARED_THING = override
                DB_URL = "postgresql://somewhere:5432/mydb"
                DB_USER = admin
                DB_PASSWORD = "Password123!"
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
        val exception = shouldThrow<IllegalArgumentException> {
            configSampleWorkflow().build(params)
        }
        exception.cause.shouldBeInstanceOf<MissingKotlinParameterException>()
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
        parsedParams shouldBe parsedCompleteParams
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
            env = mapOf(
                "MY_SHARED_THING" to "override",
                "DB_URL" to "postgresql://somewhere:5432/mydb",
                "DB_USER" to "admin",
                "DB_PASSWORD" to "Password123!"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-2",
                diskSize = Capacity(30, CapacityType.GB)
            )
        )

        workflowConfig.tasks["sample2"] shouldBe TaskConfig(
            env = mapOf(
                "MY_SHARED_THING" to "someval"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-1",
                diskSize = Capacity(5, CapacityType.GB)
            )
        )

        workflowConfig.tasks["sample3"] shouldBe TaskConfig(
            env = mapOf(
                "MY_SHARED_THING" to "someval"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-2",
                diskSize = Capacity(10, CapacityType.GB)
            )
        )
    }

})