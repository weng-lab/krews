package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.*
import io.kotlintest.matchers.numerics.shouldBeLessThanOrEqual
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import krews.config.createParamsForConfig
import krews.config.createWorkflowConfig
import krews.core.Params
import krews.core.WorkflowRunner
import krews.core.workflow
import krews.executor.LocallyDirectedExecutor
import krews.executor.local.LocalExecutor
import mu.KotlinLogging
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import reactor.core.scheduler.Schedulers
import java.lang.Math.random
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import kotlin.math.max

private val log = KotlinLogging.logger {}

class ConcurrencyTest : StringSpec(){
    override fun tags() = setOf(Unit)

    private val testDir = Paths.get("concurrency-test")!!
    private val baseConfig =
        """
        local {
            local-base-dir = $testDir
        }
        """.trimIndent()
    private val taskParConfig =
        """
        $baseConfig
        task.task1 {
            parallelism = 10
        }
        """.trimIndent()
    private val workflowParConfig =
        """
        $baseConfig
        parallelism = 10
        """.trimIndent()

    private lateinit var outputsCaptured: Mono<List<Int>>
    private val testWorkflow = workflow("test") {
        val i = (1..30).toFlux()
        val task1 = task<Int, Int>("task1") {
            dockerImage = "task1"
            input = i
            outputFn { inputItem }
            commandFn { "" }
        }
        val task2 = task<Int,Int>("task2") {
            dockerImage = "task2"
            input = task1.output
            outputFn { inputItem }
            commandFn { "" }
        }
        outputsCaptured = task2.output.buffer().toMono()
    }

    override fun afterTest(description: Description, result: TestResult) {
        // Clean up temporary dirs
        Files.walk(testDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }

    init {
        "Per task parallelism should be enforced" {
            val (executor , runner) = runWorkflow(taskParConfig)
            val task1Tracker = AtomicInteger()
            val task1MaxConcurrent = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), "task1", any(), any(), any(), any(), any(), any())
            } answers {
                val runningTasks = task1Tracker.incrementAndGet()
                runningTasks shouldBeLessThanOrEqual 10
                task1MaxConcurrent.updateAndGet { current -> max(current, runningTasks) }
                Thread.sleep(500)
                task1Tracker.decrementAndGet()
            }
            runner.run()
            task1MaxConcurrent.get() shouldBe 10
            outputsCaptured.block() shouldBe (1..30).toList()
        }

        "Per workflow parallelism should be enforced" {
            val (executor , runner) = runWorkflow(workflowParConfig)
            val taskTracker = AtomicInteger()
            val taskMaxConcurrent = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
            } answers {
                val runningTasks = taskTracker.incrementAndGet()
                runningTasks shouldBeLessThanOrEqual 10
                taskMaxConcurrent.updateAndGet { current -> max(current, runningTasks) }
                Thread.sleep(500)
                taskTracker.decrementAndGet()
            }
            runner.run()
            taskMaxConcurrent.get() shouldBe 10
            outputsCaptured.block() shouldBe (1..30).toList()
        }

    }

    private data class ExecutorAndRunner(val executor: LocallyDirectedExecutor, val runner: WorkflowRunner)

    private fun runWorkflow(config: String): ExecutorAndRunner {
        val parsedConfig = ConfigFactory.parseString(config)
        val workflow = testWorkflow.build(Params(createParamsForConfig(parsedConfig)))
        val workflowConfig = createWorkflowConfig(parsedConfig, workflow)
        val executor = mockk<LocallyDirectedExecutor>(relaxed = true)
        val localExecutor = LocalExecutor(workflowConfig)
        every {
            executor.prepareDatabaseFile()
        } answers {
            localExecutor.prepareDatabaseFile()
        }
        val runner = WorkflowRunner(workflow, workflowConfig, executor)
        return ExecutorAndRunner(executor, runner)
    }

}