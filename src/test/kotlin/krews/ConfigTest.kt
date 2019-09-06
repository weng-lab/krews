package krews

import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.typesafe.config.ConfigFactory
import krews.config.*
import krews.core.*
import krews.file.*
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*
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

        working-dir = /test
        parallelism = 5

        google = {
            project-id = test-project
            bucket = test-bucket
        }

        task.default {
            grouping = 3
            params {
                my-shared-thing = someval
            }

            google {
                machine-type = n1-standard-2
                disk-size = 10GB
            }
        }

        # Overrides for sample2
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
            grouping = 1
        }
        """.trimIndent()

class ConfigTests {
    @Test fun `Parsing params without missing not-nullable field should throw exception`() {
        val config = ConfigFactory.parseString(brokenParamsConfig)
        val params = createParamsForConfig(config)
        assertThatExceptionOfType(MissingKotlinParameterException::class.java).isThrownBy {
            configSampleWorkflow().build(params)
        }
    }

    @Test fun `Parsing workflow params with defaults and nullability should work`() {
        val config = ConfigFactory.parseString(sparseParamsConfig)
        val params = createParamsForConfig(config)
        configSampleWorkflow().build(params)
        assertThat(parsedParams).isEqualTo(parsedSparseParams)
    }

    @Test fun `Parsing workflow params with all fields should work`() {
        val config = ConfigFactory.parseString(completeParamsConfig)
        val params = createParamsForConfig(config)
        configSampleWorkflow().build(params)
        assertThat(parsedParams!!.withDefault).isEqualTo(parsedCompleteParams.withDefault)
        assertThat(parsedParams!!.withoutDefault).isEqualTo(parsedCompleteParams.withoutDefault)
        assertThat(parsedParams!!.complex).isEqualTo(parsedCompleteParams.complex)
        assertThat(parsedParams!!.nullableFile!!.path).isEqualTo(parsedCompleteParams.nullableFile!!.path)
        assertThat(parsedParams!!.nullableFile!!).isInstanceOf(InputFile::class.java)
        assertThat(parsedParams!!.list).isEqualTo(parsedCompleteParams.list)
        assertThat(parsedParams!!.map).isEqualTo(parsedCompleteParams.map)
    }

    @Test fun `createWorkflowConfig should create complete complex task-based configurations`() {
        val config = ConfigFactory.parseString(completeTestConfig)
        val params = createParamsForConfig(config)
        val workflow = configSampleWorkflow().build(params)
        val workflowConfig = createWorkflowConfig(config, workflow)
        assertThat(workflowConfig.google).isEqualTo(GoogleWorkflowConfig(
            projectId = "test-project",
            bucket = "test-bucket"
        ))
        assertThat(workflowConfig.local).isNull()
        assertThat(workflowConfig.tasks["sample"]).isEqualTo(TaskConfig(
            params = mapOf(
                "my-shared-thing" to "override",
                "db-url" to "postgresql://somewhere:5432/mydb",
                "db-user" to "admin",
                "db-password" to "Password123!"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-2",
                diskSize = 30.GB
            ),
            grouping = 1
        ))

        assertThat(workflowConfig.tasks["sample2"]).isEqualTo(TaskConfig(
            params = mapOf(
                "my-shared-thing" to "someval"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-1",
                diskSize = 5.GB
            ),
            grouping = 3
        ))

        assertThat(workflowConfig.tasks["sample3"]).isEqualTo(TaskConfig(
            params = mapOf(
                "my-shared-thing" to "someval"
            ),
            google = GoogleTaskConfig(
                machineType = "n1-standard-2",
                diskSize = 10.GB
            ),
            grouping = 3
        ))
    }

    @Test fun `createWorkflowConfig should create configuration without workflow`() {
        val config = ConfigFactory.parseString(completeTestConfig)
        val workflowConfig = createWorkflowConfig(config, null)
        assertThat(workflowConfig).isNotNull
    }

}