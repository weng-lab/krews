package krews

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor

data class File(val path: String)

class Task<I : Any, O : Any> internal constructor(
        val workflow: Workflow,
        val name: String,
        val labels: List<String> = listOf(),
        val image: String,
        val input: Flux<I>,
        private val outputFn: (inputItem: I) -> O,
        private val scriptFn: (inputItem: I) -> String,
        internal val outputClass: Class<O>) {

    val output: Flux<O> = TopicProcessor.create<O>()

    internal lateinit var executeFn: (script: String, inputItem: Any, outputItem: Any?) -> Unit

    internal fun connect() {
        val processed = input.map { processInput(it) }
        processed.subscribe(output as TopicProcessor<O>)
    }

    private fun processInput(inputItem: I): O {
        val outputItem = outputFn(inputItem)
        val script = scriptFn(inputItem)
        executeFn(script, inputItem, outputItem)
        return outputItem
    }
}

val workflow = Workflow()

inline fun <I : Any, reified O : Any> task(name: String, noinline init: TaskBuilder<I, O>.() -> Unit): Task<I, O> = workflow.task(name, init)

class TaskBuilder<I : Any, O : Any> @PublishedApi internal constructor(
        private val workflow: Workflow,
        private val name: String,
        private val outputClass: Class<O>) {

    private var input: Publisher<I>? = null
    private var outputFn: ((inputItem: I) -> O)? = null
    private var scriptFn: ((inputItem: I) -> String)? = null
    var image: String? = null
    var labels: List<String> = listOf()

    fun image(image: String) {
        this.image = image
    }

    fun labels(vararg labels: String) {
        this.labels = listOf(*labels)
    }

    fun input(init: () -> Publisher<I>) {
        input = init()
    }

    fun outputFn(init: InputItemContext<I>.() -> O) {
        outputFn = { inputItem ->
            InputItemContext(inputItem).init()
        }
    }

    fun scriptFn(init: InputItemContext<I>.() -> String) {
        scriptFn = { inputItem ->
            InputItemContext(inputItem).init()
        }
    }

    @PublishedApi
    internal fun build(): Task<I, O> {
        val inputNN: Publisher<I> = checkNotNull(input)
        val inFlux: Flux<I> = if (inputNN is Flux) inputNN else Flux.from(inputNN)
        return Task(
            workflow = workflow,
            name = name,
            image = checkNotNull(image),
            labels = this.labels,
            input = inFlux,
            outputFn = checkNotNull(outputFn),
            scriptFn = checkNotNull(scriptFn),
            outputClass = outputClass
        )
    }

}

data class InputItemContext<I : Any>(val inputItem: I)
