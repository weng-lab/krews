package krews.core

import com.fasterxml.jackson.module.kotlin.convertValue
import krews.config.configMapper
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class TaskBuilder<I : Any, O : Any> @PublishedApi internal constructor(
    private val name: String,
    private val outputClass: Class<O>
) {
    lateinit var dockerImage: String
    var dockerDataDir: String = DEFAULT_DOCKER_DATA_DIR
    lateinit var input: Publisher<out I>
    private lateinit var outputFn: ((inputItemContext: InputItemContext<I>) -> O)
    private lateinit var commandFn: ((inputItemContext: InputItemContext<I>) -> String)
    var labels: List<String> = listOf()


    fun outputFn(init: InputItemContext<I>.() -> O) {
        outputFn = { inputItemContext ->
            inputItemContext.init()
        }
    }

    fun commandFn(init: InputItemContext<I>.() -> String) {
        commandFn = { inputItemContext ->
            inputItemContext.init()
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
            outputClass = outputClass
        )

    }
}

data class InputItemContext<I : Any>(val inputItem: I, @PublishedApi internal val rawParams: Map<String, Any>) {
    @PublishedApi internal var cachedParams: Any? = null

    inline fun <reified P> taskParams(): P {
        if (cachedParams == null) {
            cachedParams = configMapper.convertValue<P>(rawParams)
        }
        return cachedParams as P
    }
}
