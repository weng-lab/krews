package krews

import com.typesafe.config.ConfigFactory
import io.mockk.*
import kotlinx.coroutines.delay
import krews.config.*
import krews.core.*
import krews.executor.LocallyDirectedExecutor
import krews.util.*
import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import reactor.core.publisher.*
import java.nio.file.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

private val log = KotlinLogging.logger {}

class ConcurrencyTest {

    private val testDir = Paths.get("concurrency-test")!!
    private val baseConfig =
        """
        working-dir = "$testDir"
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
        val i = (1..15).toFlux()
        val task1 = task<Int, Int>("task1", i) {
            dockerImage = "task1"
            output = input
            command = ""
        }
        val task2 = task<Int, Int>("task2", task1) {
            dockerImage = "task2"
            output = input
            command = ""
        }
        val task3 = task<Int, Int>("task3", task2) {
            dockerImage = "task3"
            output = input
            command = ""
        }
        outputsCaptured = task3.buffer().toMono()
    }

    @AfterAll
    fun afterTests() = deleteDir(testDir)

    @Test fun `Will fail with asynchronous exceptions`() {
        val (executor, runner) = runWorkflow(workflowParConfig)
        val task1Count = AtomicInteger()

        coEveryMatchTaskRun(executor) { it.dockerImage == "task1" } coAnswers {
            val task1s = task1Count.incrementAndGet()
            if (task1s > 5) {
                delay(500)
                throw Exception("Test exception")
            }
        }

        coEveryMatchTaskRun(executor) { it.dockerImage == "task2" || it.dockerImage == "task3" } just Runs

        runner.run()
        assertThat(outputsCaptured.block()?.toSet()?.size).isEqualTo(5)
    }

    @Test fun `Per task parallelism should be enforced`() {
        val (executor, runner) = runWorkflow(taskParConfig)
        val task1Count = AtomicInteger()
        val task1Latch = CountDownLatch(10)
        val task1LatchPostCheck = CountDownLatch(10)
        coEveryMatchTaskRun(executor) { it.dockerImage == "task1" } coAnswers {
            val task1s = task1Count.incrementAndGet()
            task1Latch.countDown()
            if (task1s <= 10) {
                while (!task1Latch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                delay(500)
                assertThat(task1Count.get()).isEqualTo(10)
                task1LatchPostCheck.countDown()
                while (!task1LatchPostCheck.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
            }
        }

        coEveryMatchTaskRun(executor) { it.dockerImage == "task2" || it.dockerImage == "task3" } just Runs

        runner.run()
        assertThat(outputsCaptured.block()?.toSet()).isEqualTo((1..15).toSet())
    }

    @Test fun `Per workflow parallelism should be enforced`() {
        val (executor, runner) = runWorkflow(workflowParConfig)
        val task1Latch = CountDownLatch(10)
        val task1LatchPostCheck = CountDownLatch(10)
        val taskCount = AtomicInteger()
        coEvery {
            executor.executeTask(any(), any(), any(), any())
        } coAnswers {
            val task1s = taskCount.incrementAndGet()
            task1Latch.countDown()
            if (task1s <= 10) {
                while (!task1Latch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                delay(500)
                assertThat(taskCount.get()).isEqualTo(10)
                task1LatchPostCheck.countDown()
                while (!task1LatchPostCheck.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
            }
        }
        runner.run()
        assertThat(outputsCaptured.block()?.toSet()).isEqualTo((1..15).toSet())
    }

    @Test
    fun `Tasks should be run depth-first`() {
        val (executor, runner) = runWorkflow(baseConfig)
        val task1Count = AtomicInteger()
        val task2Latch = CountDownLatch(1)

        coEveryMatchTaskRun(executor) { it.dockerImage == "task1" } coAnswers {
            val task1s = task1Count.incrementAndGet()
            if (task1s > 1) {
                //val timedOut = !task2Latch.await(500, TimeUnit.MILLISECONDS)
                //timedOut shouldBe false
            }
            //CompletableFuture.completedFuture(null)
        }

        coEveryMatchTaskRun(executor) { it.dockerImage == "task2" } coAnswers {
            task2Latch.countDown()
        }

        coEveryMatchTaskRun(executor) { it.dockerImage == "task3" } just Runs

        runner.run()
        assertThat(outputsCaptured.block()?.toSet()).isEqualTo((1..15).toSet())
    }

    @Test fun `If one task run fails all others that aren't downstream should complete`() {
        val (executor, runner) = runWorkflow(baseConfig)
        val task1Count = AtomicInteger()
        val task1BeforeErrorLatch = CountDownLatch(5)
        val alltasksLatch = CountDownLatch(14)
        coEveryMatchTaskRun(executor) { it.dockerImage == "task1" } coAnswers {
            val task1s = task1Count.incrementAndGet()
            if (task1s < 6) {
                task1BeforeErrorLatch.countDown()
            }
            if (task1s == 6) {
                // Ensure 5 task runs complete before throwing an error.
                while (!task1BeforeErrorLatch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                task1BeforeErrorLatch.await()
                throw Exception("Test Error")
            } else {
                alltasksLatch.countDown()
                while (!alltasksLatch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                delay(1000)
            }
        }

        val task2Count = AtomicInteger()
        coEveryMatchTaskRun(executor) { it.dockerImage == "task2" } coAnswers {
            val count = task2Count.incrementAndGet()
            if (count == 10) {
                throw Exception("Test Error 2")
            }
        }

        val task3Count = AtomicInteger()
        coEveryMatchTaskRun(executor) { it.dockerImage == "task3" } coAnswers {
            // Make sure onShutdown doesn't happen before the workflow is complete
            task3Count.incrementAndGet()
        }

        runner.run()
        assertThat(task1Count.get()).isEqualTo(15)
        assertThat(task2Count.get()).isEqualTo(14)
        assertThat(task3Count.get()).isEqualTo(13)
    }

    private data class ExecutorAndRunner(val executor: LocallyDirectedExecutor, val runner: WorkflowRunner)

    private fun runWorkflow(config: String): ExecutorAndRunner {
        val parsedConfig = ConfigFactory.parseString(config)
        val workflow = testWorkflow.build(createParamsForConfig(parsedConfig))
        val workflowConfig = createWorkflowConfig(parsedConfig, workflow)
        val executor = mockk<LocallyDirectedExecutor>(relaxed = true)
        val runner = WorkflowRunner(workflow, workflowConfig, executor)
        return ExecutorAndRunner(executor, runner)
    }
}