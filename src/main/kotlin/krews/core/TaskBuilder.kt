package krews.core

import krews.file.File
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class TaskBuilder<I : Any, O : Any> @PublishedApi internal constructor(
    private val name: String,
    private val inputClass: Class<I>,
    private val outputClass: Class<O>
) {
    lateinit var dockerImage: String
    var dockerDataDir: String = DEFAULT_DOCKER_DATA_DIR
    lateinit var input: Publisher<out I>
    private lateinit var outputFn: ((inputElementContext: InputElementContext<I>) -> O)
    private lateinit var commandFn: ((inputElementContext: InputElementContext<I>) -> String)
    var labels: List<String> = listOf()


    fun outputFn(init: InputElementContext<I>.() -> O) {
        outputFn = { inputElementContext ->
            inputElementContext.init()
        }
    }

    fun commandFn(init: InputElementContext<I>.() -> String) {
        commandFn = { inputElementContext ->
            inputElementContext.init().trimIndent()
        }
    }

    @PublishedApi
    internal fun build(): Task<I, O> {
        val inputNN: Publisher<out I> = checkNotNull(input)
        val inFlux: Flux<out I> = if (inputNN is Flux) inputNN else Flux.from(inputNN)
        return Task(
            name = name,
            labels = this.labels,
            input = inFlux,
            dockerImage = dockerImage,
            dockerDataDir = dockerDataDir,
            outputFn = outputFn,
            commandFn = commandFn,
            inputClass = inputClass,
            outputClass = outputClass
        )
    }

    val File.dockerPath: String get() = "$dockerDataDir/${this.path}"
}

data class InputElementContext<I : Any>(val inputEl: I)
