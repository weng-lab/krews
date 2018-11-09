package krews.core

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

open class Workflow {
    lateinit var name: String
        internal set

    lateinit var params: Map<String, Any>
        internal set

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

}

@PublishedApi internal val defaultWorkflow = Workflow()