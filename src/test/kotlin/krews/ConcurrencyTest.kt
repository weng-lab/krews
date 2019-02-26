package krews

import com.typesafe.config.ConfigFactory
import io.kotlintest.Description
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import krews.config.createParamsForConfig
import krews.config.createWorkflowConfig
import krews.core.WorkflowRunner
import krews.core.workflow
import krews.executor.LocallyDirectedExecutor
import mu.KotlinLogging
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

private val log = KotlinLogging.logger {}

class ConcurrencyTest : StringSpec(){
    override fun tags() = setOf(Unit)

    private val testDir = Paths.get("concurrency-test")!!
    private val baseConfig =
        """
        local-files-base-dir = "$testDir"
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
        val task3 = task<Int,Int>("task3", task2.outputPub) {
            dockerImage = "task3"
            output = input
            command = ""
        }
        outputsCaptured = task3.outputPub.buffer().toMono()
    }

    override fun afterTest(description: Description, result: TestResult) {
        // Clean up temporary dirs
        if (Files.isDirectory(testDir)) {
            Files.walk(testDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.delete(it) }
        }
    }

    private val testExecutor = Executors.newCachedThreadPool()

    init {

        "Will fail with asynchronous exceptions" {
            val (executor , runner) = runWorkflow(workflowParConfig)
            val task1Count = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1" }, any(), any(), any(), any())
            } answers {
                val task1s = task1Count.incrementAndGet()
                CompletableFuture.supplyAsync {
                    if (task1s > 5) {
                        Thread.sleep(500)
                        throw Exception("Test exception")
                    } else {
                        null
                    }
                }
            }

            every { executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" || it.dockerImage == "task3" }, any(), any(), any(), any())
            } answers { CompletableFuture.completedFuture(null) }

            runner.run()
            outputsCaptured.block()?.toSet()?.size shouldBe 5
        }

        "Per task parallelism should be enforced" {
            val (executor , runner) = runWorkflow(taskParConfig)
            val task1Count = AtomicInteger()
            val task1Latch = CountDownLatch(11)
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1" }, any(), any(), any(), any())
            } answers {
                val task1s = task1Count.incrementAndGet()
                task1Latch.countDown()
                CompletableFuture.supplyAsync( Supplier {
                    val timedOut = !task1Latch.await(1000, TimeUnit.MILLISECONDS)
                    if (task1s <= 10) {
                        timedOut shouldBe true
                    } else {
                        timedOut shouldBe false
                    }
                }, testExecutor)
            }

            every { executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" || it.dockerImage == "task3" }, any(), any(), any(), any())
            } answers { CompletableFuture.completedFuture(null) }

            runner.run()
            outputsCaptured.block()?.toSet() shouldBe (1..15).toSet()
        }

        "f:Per workflow parallelism should be enforced" {
            val (executor , runner) = runWorkflow(workflowParConfig)
            val first10Latch = CyclicBarrier(10)
            val taskCount = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), any(), any(), any(), any(), any())
            } answers {
                val task1s = taskCount.incrementAndGet()
                CompletableFuture.supplyAsync(Supplier {
                    if (task1s <= 10) {
                        first10Latch.await()
                        Thread.sleep(1000)
                        taskCount.get() shouldBe 10
                    }
                }, testExecutor)
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
                    CompletableFuture.supplyAsync(Supplier {
                        val timedOut = !task2Latch.await(500, TimeUnit.MILLISECONDS)
                        timedOut shouldBe false
                        null

                    }, testExecutor)
                }
                CompletableFuture.completedFuture(null)
            }

            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" }, any(), any(), any(), any())
            } answers {
                task2Latch.countDown()
                CompletableFuture.completedFuture(null)
            }

            every { executor.executeTask(any(), any(), any(), match { it.dockerImage == "task3" }, any(), any(), any(), any())
            } answers { CompletableFuture.completedFuture(null) }

            runner.run()
            outputsCaptured.block()?.toSet() shouldBe (1..15).toSet()
        }

        "If one task run fails all others that aren't downstream should complete" {
            val (executor , runner) = runWorkflow(baseConfig)
            val task1Count = AtomicInteger()
            val task1BeforeErrorLatch = CountDownLatch(5)
            val alltasksBarrier = CyclicBarrier(14)
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task1" }, any(), any(), any(), any())
            } answers {
                val task1s = task1Count.incrementAndGet()
                if (task1s < 6) {
                    task1BeforeErrorLatch.countDown()
                }
                if (task1s == 6) {
                    CompletableFuture.supplyAsync(Supplier {
                        // Ensure 5 task runs complete before throwing an error.
                        task1BeforeErrorLatch.await()
                        // Supress type inference warning (Expect Unit, got Nothing)
                        if (true) {
                            throw Exception("Test Error")
                        }
                    }, testExecutor)
                } else {
                    CompletableFuture.supplyAsync(Supplier {
                        alltasksBarrier.await()
                        Thread.sleep(1000)
                    }, testExecutor)
                }
            }

            val task2Count = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task2" }, any(), any(), any(), any())
            } answers {
                val count = task2Count.incrementAndGet()
                if (count == 10) {
                    throw Exception("Test Error 2")
                }
                CompletableFuture.completedFuture(null)
            }

            val task3Count = AtomicInteger()
            every {
                executor.executeTask(any(), any(), any(), match { it.dockerImage == "task3" }, any(), any(), any(), any())
            } answers {
                // Make sure onShutdown doesn't happen before the workflow is complete
                task3Count.incrementAndGet()
                CompletableFuture.completedFuture(null)
            }

            runner.run()
            task1Count.get() shouldBe 15
            task2Count.get() shouldBe 14
            task3Count.get() shouldBe 13
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