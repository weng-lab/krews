package krews.core

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor

class FileImport<I : Any> internal constructor(val name: String, val input: Flux<I>, val dockerDataDir: String) {
    val output: Flux<I> = TopicProcessor.create<I>()
    internal lateinit var executeFn: (inputItem: I) -> Unit

    internal fun connect() {
        val processed = input.map { processInput(it) }
        processed.subscribe(output as TopicProcessor<I>)
    }

    private fun processInput(inputItem: I): I {
        executeFn(inputItem)
        return inputItem
    }
}

fun <I : Any> fileImport(name: String, input: Publisher<I>, dockerDataDir: String?): FileImport<I> =
    defaultWorkflow.fileImport(name, input, dockerDataDir)