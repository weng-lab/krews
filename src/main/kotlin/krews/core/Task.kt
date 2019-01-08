package krews.core

import krews.config.LimitedParallelism
import krews.config.Parallelism
import krews.config.TaskConfig
import krews.config.UnlimitedParallelism
import mu.KotlinLogging
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.util.concurrent.Queues
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.function.Supplier

const val DEFAULT_DOCKER_DATA_DIR = "/data"

private val log = KotlinLogging.logger {}

class Task<I : Any, O : Any> @PublishedApi internal constructor(
    val name: String,
    val inputPub: Publisher<out I>,
    val labels: List<String> = listOf(),
    internal val inputClass: Class<I>,
    internal val outputClass: Class<O>,
    private val taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit
) {
    val outputPub: Flux<O> = TopicProcessor.create<O>("$name-topic", 1024)

    internal fun connect(taskConfig: TaskConfig?,
                         executeFn: (TaskRunContext<I, O>) -> O,
                         pool: ExecutorService) {
        val rawTaskParams = taskConfig?.params ?: mapOf()
        val inputFlux: Flux<out I> = if (inputPub is Flux) inputPub else Flux.from(inputPub)
        val processed = inputFlux.flatMap({
            Mono.fromFuture(CompletableFuture.supplyAsync(Supplier {
                processInput(it, rawTaskParams, executeFn)
            }, pool))
        }, parToMaxConcurrency(taskConfig?.parallelism)).onErrorContinue { t: Throwable, _ ->
            log.error(t) { }
        }

        processed.subscribe(outputPub as TopicProcessor)
    }

    private fun processInput(input: I, rawTaskParams: Map<String, Any>, executeFn: (TaskRunContext<I, O>) -> Any): O {
        log.info { "In processInput for task $name" }
        val taskRunContextBuilder = TaskRunContextBuilder(input, rawTaskParams, outputClass)
        taskRunContextBuilder.taskRunContextInit()
        val taskRunContext = taskRunContextBuilder.build()
        @Suppress("UNCHECKED_CAST")
        return executeFn(taskRunContext) as O
    }
}

internal fun parToMaxConcurrency(par: Parallelism?) = when (par) {
    is LimitedParallelism -> par.limit
    // If "Unlimited" use an arbitrarily high number (that's an exponent of two) as required by Reactor's flatMap
    null, is UnlimitedParallelism -> 262144
}