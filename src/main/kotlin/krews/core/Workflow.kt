package krews.core

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono

open class Workflow {
    lateinit var name: String
        internal set

    internal val paramsMono: Mono<Map<String, Any>> = Mono.defer { params.toMono() }
    internal lateinit var params: Map<String, Any>

    @PublishedApi internal val tasks = mutableListOf<Task<*, *>>()
    internal val fileImports =  mutableListOf<FileImport<*>>()

    internal constructor()
    constructor(name: String) {
        this.name = name
    }

    inline fun <I : Any, reified O : Any> task(name: String, init: TaskBuilder<I, O>.() -> Unit): Task<I, O> {
        val builder = TaskBuilder<I, O>(this, name, O::class.java)
        builder.init()
        val task = builder.build()
        this.tasks.add(task)
        return task
    }

    fun <I : Any> fileImport(name: String, input: Publisher<I>, dockerDataDir: String?): FileImport<I> {
        val inFlux: Flux<I> = if (input is Flux) input else Flux.from(input)
        val fileImport = FileImport(name, inFlux, dockerDataDir ?: DEFAULT_DOCKER_DATA_DIR)
        this.fileImports.add(fileImport)
        return fileImport
    }

    @Suppress("UNCHECKED_CAST")
    fun <P> params(key: String): Mono<P> {
        return this.paramsMono.map { it[key] as P }
    }

}

@PublishedApi internal val defaultWorkflow = Workflow()
fun <P> params(key: String) = defaultWorkflow.params<P>(key)