package krews.core

import krews.config.LimitedParallelism
import krews.config.Parallelism
import krews.config.TaskConfig
import krews.config.UnlimitedParallelism
import reactor.core.publisher.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.function.Supplier

const val DEFAULT_DOCKER_DATA_DIR = "/data"
const val DEFAULT_TASK_PARALLELISM = 256

class Task<I : Any, O : Any> internal constructor(
    val name: String,
    val labels: List<String> = listOf(),
    val input: Flux<out I>,
    val dockerImage: String,
    val dockerDataDir: String,
    private val outputFn: (inputItemContext: InputItemContext<I>) -> O,
    private val commandFn: (inputItemContext: InputItemContext<I>) -> String,
    internal val outputClass: Class<O>
) {
    val output: Flux<O> = TopicProcessor.create<O>()

    internal fun connect(taskConfig: TaskConfig?,
                         executeFn: (command: String, inputItem: I, outputItem: O) -> Unit,
                         executorService: ExecutorService) {
        val processed = input.flatMapSequential({
            Mono.fromFuture(CompletableFuture.supplyAsync(Supplier {
                processInput(it, taskConfig, executeFn)
            }, executorService))
        }, parToMaxConcurrency(taskConfig?.parallelism))
        processed.subscribe(output as TopicProcessor)
    }

    private fun processInput(inputItem: I,
                             taskConfig: TaskConfig?,
                             executeFn: (command: String, inputItem: I, outputItem: O) -> Unit): O {
        val taskParams = taskConfig?.params ?: mapOf()
        val inputItemContext = InputItemContext(inputItem, taskParams)
        val outputItem = outputFn(inputItemContext)
        val command = commandFn(inputItemContext)
        executeFn(command, inputItemContext.inputItem, outputItem)
        return outputItem
    }
}

internal fun parToMaxConcurrency(par: Parallelism?) = when (par) {
    is LimitedParallelism -> par.limit
    null, is UnlimitedParallelism -> DEFAULT_TASK_PARALLELISM
}