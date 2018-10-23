package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import krews.config.*
import reactor.core.publisher.toMono
/*
object ConfigSampleWF : Workflow("config-sample") {
    val sample = task<String, String>("sample") {
        image("test")
        input { "".toMono() }
        outputFn { "" }
        scriptFn { "" }
    }

    val sample2 = task<String, String>("sample2") {
        labels("small", "fast")
        image("test")
        input { "".toMono() }
        outputFn { "" }
        scriptFn { "" }
    }

    val sample3 = task<String, String>("sample3") {
        labels("small", "slow")
        image("test")
        input { "".toMono() }
        outputFn { "" }
        scriptFn { "" }
    }
}

val localExecWorkflowConfig = WorkflowConfig(
    params = mapOf("my-param" to "my-value"),
    localExec = LocalExecConfig(
        localBaseDir = "/tmp",
        docker = DockerConfig(
            uri = "unix:///var/run/docker.sock",
            certificatesPath = "/Users/test/.docker/boot2docker-vm/",
            connectTimeout = 5000,
            readTimeout = 30000,
            connectionPoolSize = 100
        )
    ),
    tasks = mapOf(
        "sample" to TaskConfig(
            env = mapOf(
                "MY_SHARED_THING" to "someval",
                "DB_URL" to "postgresql://somewhere:5432/mydb",
                "DB_USER" to "admin",
                "DB_PASSWORD" to "Password123!"
            )
        ),
        "sample2" to TaskConfig(
            env = mapOf(
                "MY_SHARED_THING" to "someval"
            )
        ),
        "sample3" to TaskConfig(
            env = mapOf(
                "MY_SHARED_THING" to "someval"
            )
        )
    )
)

class ConfigTests : StringSpec({
    "createWorkflowConfig should take a HOCON Config and Workflow and produce an appropriate WorkflowConfig class" {
        val workflowConfig = createWorkflowConfig(
            ConfigFactory.parseResources("config-tests/local-exec.conf"),
            ConfigSampleWF
        )
        workflowConfig shouldBe localExecWorkflowConfig
    }
})*/