package krews

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class TaskBuilder<I : Any, O : Any> @PublishedApi internal constructor(
        private val workflow: Workflow,
        private val name: String,
        private val outputClass: Class<O>) {
    lateinit var dockerImage: String
    var dockerDataDir: String = "/data"
    lateinit var input: Publisher<I>
    private lateinit var outputFn: ((inputItem: I) -> O)
    private lateinit var commandFn: ((inputItem: I) -> String)
    var labels: List<String> = listOf()


    fun outputFn(init: InputItemContext<I>.() -> O) {
        outputFn = { inputItem ->
            InputItemContext(inputItem).init()
        }
    }

    fun commandFn(init: InputItemContext<I>.() -> String) {
        commandFn = { inputItem ->
            InputItemContext(inputItem).init()
        }
    }

    @PublishedApi internal fun build(): Task<I, O> {
        val inputNN: Publisher<I> = checkNotNull(input)
        val inFlux: Flux<I> = if (inputNN is Flux) inputNN else Flux.from(inputNN)
        return Task(
            workflow = workflow,
            name = name,
            labels = this.labels,
            input = inFlux,
            dockerImage = dockerImage,
            dockerDataDir = dockerDataDir,
            outputFn = outputFn,
            commandFn = commandFn,
            outputClass = outputClass)

    }
}

data class InputItemContext<I : Any>(val inputItem: I)
