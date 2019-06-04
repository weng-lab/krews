package krews.core

import krews.config.*
import krews.file.getOutputFilesForObject
import mu.KotlinLogging
import org.reactivestreams.Publisher
import reactor.core.publisher.*
import reactor.util.concurrent.Queues

const val DEFAULT_DOCKER_INPUTS_DIR = "/inputs"
const val DEFAULT_DOCKER_OUTPUTS_DIR = "/outputs"

private val log = KotlinLogging.logger {}

class Task<I : Any, O : Any> @PublishedApi internal constructor(
    val name: String,
    val inputPub: Publisher<out I>,
    val labels: List<String> = listOf(),
    private val inputClass: Class<I>,
    private val outputClass: Class<O>,
    private val maintainOrder: Boolean,
    private val taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit
) {
    val outputPub: Flux<O> = TopicProcessor.create<O>("$name-topic", 1024)

    internal fun connect(taskConfig: TaskConfig?, taskRunner: TaskRunner) {
        val rawTaskParams = taskConfig?.params ?: mapOf()
        val inputFlux: Flux<out I> = if (inputPub is Flux) inputPub else Flux.from(inputPub)
        val inputToProcess = inputFlux
            .doOnTerminate { taskRunner.taskComplete(name) }

        fun processInputMono(input: I) = Mono.fromFuture(taskRunner.submit(createTaskRunContext(input, rawTaskParams)))
        val rawProcessed = if (maintainOrder) {
            inputToProcess.flatMapSequentialDelayError({ processInputMono(it) },
                parToMaxConcurrency(taskConfig?.parallelism), Queues.XS_BUFFER_SIZE)
        } else {
            inputToProcess.flatMapDelayError({ processInputMono(it) },
                parToMaxConcurrency(taskConfig?.parallelism), Queues.XS_BUFFER_SIZE)
        }

        val processed = rawProcessed
            .onErrorContinue { t: Throwable, _ -> log.error(t) { } }

        processed.subscribe(outputPub as TopicProcessor)
    }


    private fun createTaskRunContext(input: I, rawTaskParams: Map<String, Any>): TaskRunContext<I, O> {
        val taskRunContextBuilder = TaskRunContextBuilder(name, input, rawTaskParams, inputClass, outputClass)
        taskRunContextBuilder.taskRunContextInit()
        return taskRunContextBuilder.build()
    }
}

internal fun parToMaxConcurrency(par: Parallelism?) = when (par) {
    is LimitedParallelism -> par.limit
    // If "Unlimited" use an arbitrarily high number (that's an exponent of two) as required by Reactor's flatMap
    null, is UnlimitedParallelism -> 262144
}