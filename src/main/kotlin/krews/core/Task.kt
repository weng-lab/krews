package krews.core

import krews.config.LimitedParallelism
import krews.config.Parallelism
import krews.config.TaskConfig
import krews.config.UnlimitedParallelism
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.CompletableFuture
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
    private val outputFn: (inputElementContext: InputElementContext<I>) -> O,
    private val commandFn: (inputElementContext: InputElementContext<I>) -> String,
    internal val inputClass: Class<I>,
    internal val outputClass: Class<O>
) {
    val output: Flux<O> = TopicProcessor.create<O>()

    internal fun connect(taskConfig: TaskConfig?,
                         executeFn: (command: String, inputEl: I, outputEl: O) -> Unit,
                         executorService: ExecutorService) {
        val processed = input.flatMapSequential({
            Mono.fromFuture(CompletableFuture.supplyAsync(Supplier {
                processInput(it, executeFn)
            }, executorService))
        }, parToMaxConcurrency(taskConfig?.parallelism))
        processed.subscribe(output as TopicProcessor)
    }

    private fun processInput(inputEl: I,
                             executeFn: (command: String, inputEl: I, outputEl: O) -> Unit): O {
        val inputElementContext = InputElementContext(inputEl)
        val outputEl = outputFn(inputElementContext)
        val command = commandFn(inputElementContext)
        executeFn(command, inputElementContext.inputEl, outputEl)
        return outputEl
    }
}

internal fun parToMaxConcurrency(par: Parallelism?) = when (par) {
    is LimitedParallelism -> par.limit
    null, is UnlimitedParallelism -> DEFAULT_TASK_PARALLELISM
}