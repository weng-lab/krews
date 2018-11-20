package krews.core

import krews.config.TaskConfig
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor

const val DEFAULT_DOCKER_DATA_DIR = "/data"

class Task<out I : Any, O : Any> internal constructor(
    val name: String,
    val labels: List<String> = listOf(),
    val input: Flux<out I>,
    val dockerImage: String,
    val dockerDataDir: String,
    private val outputFn: (inputItemContext: InputItemContext<I>) -> O,
    private val commandFn: (inputItemContext: InputItemContext<I>) -> String,
    internal val outputClass: Class<O>) {

    val output: Flux<O> = TopicProcessor.create<O>()

    internal lateinit var executeFn: (script: String, inputItem: Any, outputItem: Any?) -> Unit
    internal lateinit var taskParams: Map<String, Any>

    internal fun connect() {
        val processed = input.map { processInput(InputItemContext(it, taskParams)) }
        processed.subscribe(output as TopicProcessor<O>)
    }

    private fun processInput(inputItemContext: InputItemContext<I>): O {
        val outputItem = outputFn(inputItemContext)
        val command = commandFn(inputItemContext)
        executeFn(command, inputItemContext, outputItem)
        return outputItem
    }
}