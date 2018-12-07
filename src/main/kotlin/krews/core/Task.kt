package krews.core

import krews.config.LimitedParallelism
import krews.config.Parallelism
import krews.config.TaskConfig
import krews.config.UnlimitedParallelism
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.function.Supplier

const val DEFAULT_DOCKER_DATA_DIR = "/data"
const val DEFAULT_TASK_PARALLELISM = 256

class Task<I : Any, O : Any> @PublishedApi internal constructor(
    val name: String,
    val inputPub: Publisher<out I>,
    val labels: List<String> = listOf(),
    internal val inputClass: Class<I>,
    internal val outputClass: Class<O>,
    private val taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit
) {
    val outputPub: Flux<O> = TopicProcessor.create<O>()

    internal fun connect(taskConfig: TaskConfig?,
                         executeFn: (TaskRunContext<I, O>) -> Unit,
                         executorService: ExecutorService) {
        val rawTaskParams = taskConfig?.params ?: mapOf()
        val inputFlux: Flux<out I> = if (inputPub is Flux) inputPub else Flux.from(inputPub)
        val processed = inputFlux.flatMapSequential({
            Mono.fromFuture(CompletableFuture.supplyAsync(Supplier {
                processInput(it, rawTaskParams, executeFn)
            }, executorService))
        }, parToMaxConcurrency(taskConfig?.parallelism))
        processed.subscribe(outputPub as TopicProcessor)
    }

    private fun processInput(input: I, rawTaskParams: Map<String, Any>, executeFn: (TaskRunContext<I, O>) -> Unit): O {
        val taskRunContextBuilder = TaskRunContextBuilder<I, O>(input, rawTaskParams)
        taskRunContextBuilder.taskRunContextInit()
        val taskRunContext = taskRunContextBuilder.build()
        executeFn(taskRunContext)
        return taskRunContext.output
    }
}

internal fun parToMaxConcurrency(par: Parallelism?) = when (par) {
    is LimitedParallelism -> par.limit
    null, is UnlimitedParallelism -> DEFAULT_TASK_PARALLELISM
}