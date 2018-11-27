package krews.core

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.function.Supplier

class FileImport<I : Any> internal constructor(val name: String, val input: Flux<I>, val dockerDataDir: String) {
    val output: Flux<I> = TopicProcessor.create<I>()

    internal fun connect(executeFn: (inputEl: I) -> Unit,
                         executorService: ExecutorService) {
        val processed = input.flatMapSequential {
            Mono.fromFuture(CompletableFuture.supplyAsync(Supplier { processInput(it, executeFn) }, executorService))
        }
        processed.subscribe(output as TopicProcessor)
    }

    private fun processInput(inputEl: I,
                             executeFn: (inputEl: I) -> Unit): I {
        executeFn(inputEl)
        return inputEl
    }
}