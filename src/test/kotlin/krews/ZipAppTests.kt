package krews

import com.typesafe.config.ConfigFactory
import io.mockk.*
import kotlinx.coroutines.delay
import krews.config.*
import krews.core.*
import krews.executor.LocallyDirectedExecutor
import krews.util.coEveryMatchTaskRun
import krews.util.deleteDir
import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import reactor.core.publisher.*
import reactor.util.function.Tuple2
import java.nio.file.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

private val log = KotlinLogging.logger {}

@Disabled
class ZipAppTests {

    private val testDir = Paths.get("merging-app-test")!!
    private val baseConfig =
        """
        local-files-base-dir = $testDir
        """.trimIndent()

    private lateinit var outputsCaptured: Mono<List<Tuple2<List<Int>, List<Int>>>>
    private val testWorkflow = workflow("test") {
        val i = (1..9).toFlux()
        val task1a = task<Int, Int>("task1a", i, maintainOrder = true) {
            dockerImage = "task1a"
            output = input
            command = ""
        }
        val task1b = task<Int, Int>("task1b", i, maintainOrder = true) {
            dockerImage = "task1b"
            output = input
            command = ""
        }
        val task2input = task1a.buffer(3)
        val task3input = task1b.buffer(3)
        val task2 = task<List<Int>, List<Int>>("task2", task2input, maintainOrder = true) {
            dockerImage = "task2"
            output = input
            command = ""
        }
        val task3 = task<List<Int>, List<Int>>("task3", task3input, maintainOrder = true) {
            dockerImage = "task3"
            output = input
            command = ""
        }

        outputsCaptured = Flux.zip(task2, task3).buffer().toMono()
    }

    @AfterAll
    fun afterTests() = deleteDir(testDir)

    @Test
    fun `If one task run fails all others that aren't downstream should complete`() {
        val (executor, runner) = runWorkflow(baseConfig)
        val task1aBeforeErrorLatch = CountDownLatch(6)
        val alltask1Latch = CountDownLatch(16)
        val task1CompleteCount = AtomicInteger(0)
        val alltask1aLatch = CountDownLatch(8)
        coEveryMatchTaskRun(executor) { it.dockerImage == "task1a" } coAnswers {
            @Suppress("UNCHECKED_CAST")
            val task1s = (this.args[3] as TaskRunContext<Int, Int>).input
            if (task1s <= 6) {
                task1aBeforeErrorLatch.countDown()
            }
            if (task1s == 6) {
                // Ensure 5 task runs complete before throwing an error.
                while (!task1aBeforeErrorLatch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                throw Exception("Test Error - task1a")
            } else {
                alltask1Latch.countDown()
                while (!alltask1Latch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                delay(1000)
                task1CompleteCount.incrementAndGet()
                alltask1aLatch.countDown()
            }
        }

        val task1bBeforeErrorLatch = CountDownLatch(3)
        coEveryMatchTaskRun(executor) { it.dockerImage == "task1b" } coAnswers {
            @Suppress("UNCHECKED_CAST")
            val task1s = (this.args[3] as TaskRunContext<Int, Int>).input
            if (task1s <= 3) {
                task1bBeforeErrorLatch.countDown()
            }
            if (task1s == 3) {
                // Ensure 2 task runs complete before throwing an error.
                while (!task1bBeforeErrorLatch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                throw Exception("Test Error - task1b")
            } else {
                alltask1Latch.countDown()
                while (!alltask1Latch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                while (!alltask1aLatch.await(100, TimeUnit.MILLISECONDS)) {
                    delay(100)
                }
                // Must be long enough that everything can shutdown
                delay(2000)
                task1CompleteCount.incrementAndGet()
            }
        }

        val task2Count = AtomicInteger()
        coEveryMatchTaskRun(executor) { it.dockerImage == "task2" } coAnswers {
            println("Task2 args ${this.args}")
            val count = task2Count.incrementAndGet()
            if (count == 2) {
                //throw Exception("Test Error - task2")
            }
            delay(1000)
        }

        coEveryMatchTaskRun(executor) { it.dockerImage == "task3" } just Runs

        runner.run()
        // When not working properly (the WorkflowRunner shuts down prematurely):
        // This sometimes fails with a SQLite exception
        // Sometimes it fails because the second tasks don't run
        assertThat(task1CompleteCount.get()).isEqualTo(16)
    }

    private data class ExecutorAndRunner(val executor: LocallyDirectedExecutor, val runner: WorkflowRunner)

    private fun runWorkflow(config: String): ExecutorAndRunner {
        val parsedConfig = ConfigFactory.parseString(config)
        val workflow = testWorkflow.build(createParamsForConfig(parsedConfig))
        val workflowConfig = createWorkflowConfig(parsedConfig)
        val taskConfigs = createTaskConfigs(parsedConfig, workflow)
        val executor = mockk<LocallyDirectedExecutor>(relaxed = true)
        // Don't spy on WorkflowRunner. It seems to mask errors.
        //val runner = spyk(WorkflowRunner(workflow, workflowConfig, executor))
        val runner = WorkflowRunner(workflow, workflowConfig, taskConfigs, executor)
        return ExecutorAndRunner(executor, runner)
    }

}
