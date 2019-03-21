package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.Description
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.just
import io.mockk.mockk
import kotlinx.coroutines.delay
import krews.config.createParamsForConfig
import krews.config.createWorkflowConfig
import krews.core.TaskRunContext
import krews.core.WorkflowRunner
import krews.core.workflow
import krews.executor.LocallyDirectedExecutor
import krews.util.Unit
import mu.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import reactor.util.function.Tuple2
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

private val log = KotlinLogging.logger {}

class ZipAppTests : StringSpec(){
    override fun tags() = setOf(Unit)

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
        val task2input = task1a.outputPub.buffer(3)
        val task3input = task1b.outputPub.buffer(3)
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

        outputsCaptured = Flux.zip(task2.outputPub, task3.outputPub).buffer().toMono()
    }

    override fun afterTest(description: Description, result: TestResult) {
        if (Files.isDirectory(testDir)) {
            // Clean up temporary dirs
            Files.walk(testDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.delete(it) }
        }
    }

    init {
        "If one task run fails all others that aren't downstream should complete" {
            val (executor , runner) = runWorkflow(baseConfig)
            val task1aBeforeErrorLatch = CountDownLatch(6)
            val alltask1Latch = CountDownLatch(16)
            val task1CompleteCount = AtomicInteger(0)
            val alltask1aLatch = CountDownLatch(8)
            coEvery {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1a" }, any(), any(), any(), any())
            } coAnswers {
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
                    while(!alltask1Latch.await(100, TimeUnit.MILLISECONDS)) {
                        delay(100)
                    }
                    delay(1000)
                    task1CompleteCount.incrementAndGet()
                    alltask1aLatch.countDown()
                }
            }

            val task1bBeforeErrorLatch = CountDownLatch(3)
            coEvery {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1b" }, any(), any(), any(), any())
            } coAnswers {
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
                    while(!alltask1Latch.await(100, TimeUnit.MILLISECONDS)) {
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
            coEvery {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" }, any(), any(), any(), any())
            } coAnswers {
                println("Task2 args ${this.args}")
                val count = task2Count.incrementAndGet()
                if (count == 2) {
                    //throw Exception("Test Error - task2")
                }
                delay(1000)
            }

            coEvery {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task3" }, any(), any(), any(), any())
            } just Runs

            runner.run()
            // When not working properly (the WorkflowRunner shuts down prematurely):
            // This sometimes fails with a SQLite exception
            // Sometimes it fails because the second tasks don't run
            task1CompleteCount.get() shouldBe 16
        }
    }

    private data class ExecutorAndRunner(val executor: LocallyDirectedExecutor, val runner: WorkflowRunner)

    private fun runWorkflow(config: String): ExecutorAndRunner {
        val parsedConfig = ConfigFactory.parseString(config)
        val workflow = testWorkflow.build(createParamsForConfig(parsedConfig))
        val workflowConfig = createWorkflowConfig(parsedConfig, workflow)
        val executor = mockk<LocallyDirectedExecutor>(relaxed = true)
        // Don't spy on WorkflowRunner. It seems to mask errors.
        //val runner = spyk(WorkflowRunner(workflow, workflowConfig, executor))
        val runner = WorkflowRunner(workflow, workflowConfig, executor)
        return ExecutorAndRunner(executor, runner)
    }

}
