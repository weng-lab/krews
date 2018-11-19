package krews.core

import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor

const val DEFAULT_DOCKER_DATA_DIR = "/data"

class Task<out I : Any, O : Any> internal constructor(
    val name: String,
    val labels: List<String> = listOf(),
    val input: Flux<out I>,
    val dockerImage: String,
    val dockerDataDir: String,
    private val outputFn: (inputItem: I) -> O,
    private val commandFn: (inputItem: I) -> String,
    internal val outputClass: Class<O>) {

    val output: Flux<O> = TopicProcessor.create<O>()

    internal lateinit var executeFn: (script: String, inputItem: Any, outputItem: Any?) -> Unit

    internal fun connect() {
        val processed = input.map { processInput(it) }
        processed.subscribe(output as TopicProcessor<O>)
    }

    private fun processInput(inputItem: I): O {
        val outputItem = outputFn(inputItem)
        val command = commandFn(inputItem)
        executeFn(command, inputItem, outputItem)
        return outputItem
    }
}