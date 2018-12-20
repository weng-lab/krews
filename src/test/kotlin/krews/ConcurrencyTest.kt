package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.Description
import io.kotlintest.TestResult
import io.kotlintest.matchers.numerics.shouldBeLessThan
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import krews.config.createParamsForConfig
import krews.config.createWorkflowConfig
import krews.core.WorkflowRunner
import krews.core.workflow
import krews.executor.LocallyDirectedExecutor
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ConcurrencyTest : StringSpec(){
    override fun tags() = setOf(Unit)

    private val testDir = Paths.get("concurrency-test")!!
    private val baseConfig =
        """
        local-files-base-dir = $testDir
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
        val task2 = task<Int,Int>("task2", task1.outputPub) {
            dockerImage = "task2"
            output = input
            command = ""
        }
        outputsCaptured = task2.outputPub.buffer().toMono()
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
            val task1Count = AtomicInteger()
            val task1Latch = CountDownLatch(11)
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1" }, any(), any(), any(), any())
            } answers {
                val task1s = task1Count.incrementAndGet()
                task1Latch.countDown()
                val timedOut = !task1Latch.await(500, TimeUnit.MILLISECONDS)
                if (task1s <= 10) {
                    timedOut shouldBe true
                } else {
                    timedOut shouldBe false
                }
            }
            runner.run()
            outputsCaptured.block()?.toSet() shouldBe (1..15).toSet()
        }

        "Per workflow parallelism should be enforced" {
            val (executor , runner) = runWorkflow(workflowParConfig)
            val taskCount = AtomicInteger()
            val taskLatch = CountDownLatch(21)
            every {
                executor.executeTask(any(), any(), any(), any(), any(), any(), any(), any())
            } answers {
                val task1s = taskCount.incrementAndGet()
                taskLatch.countDown()
                val timedOut = !taskLatch.await(500, TimeUnit.MILLISECONDS)
                if (task1s <= 20) {
                    timedOut shouldBe true
                } else {
                    timedOut shouldBe false
                }
            }
            runner.run()
            outputsCaptured.block()?.toSet() shouldBe (1..15).toSet()
        }

        "Tasks should be run depth-first" {
            val (executor , runner) = runWorkflow(baseConfig)
            val task1Count = AtomicInteger()
            val task2Latch = CountDownLatch(1)
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1" }, any(), any(), any(), any())
            } answers {
                val task1s = task1Count.incrementAndGet()
                if (task1s > 1) {
                    val timedOut = !task2Latch.await(500, TimeUnit.MILLISECONDS)
                    timedOut shouldBe false
                }
            }

            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" }, any(), any(), any(), any())
            } answers {
                task2Latch.countDown()
            }

            runner.run()
            outputsCaptured.block()
        }

        "If one task run fails all others that aren't downstream should complete" {
            val (executor , runner) = runWorkflow(baseConfig)
            val task1Count = AtomicInteger()
            val task1BeforeErrorLatch = CountDownLatch(5)
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1" }, any(), any(), any(), any())
            } answers {
                val task1s = task1Count.incrementAndGet()
                if (task1s < 6) {
                    task1BeforeErrorLatch.countDown()
                }
                if (task1s == 6) {
                    // Ensure 5 task runs complete before throwing an error.
                    task1BeforeErrorLatch.await()
                    throw Exception("Test Error")
                }
            }

            val task2Count = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" }, any(), any(), any(), any())
            } answers {
                task2Count.incrementAndGet()
            }

            runner.run()
            task1Count.get() shouldBe 15
            task2Count.get() shouldBeLessThan 5
        }

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