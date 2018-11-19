package krews.core

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal val tasks: MutableMap<String, Task<*, *>> = mutableMapOf()
    private val fileImports: MutableMap<String, FileImport<*>> = mutableMapOf()
    lateinit var params: Params
        internal set

    inline fun <I : Any, reified O : Any> task(name: String, init: TaskBuilder<I, O>.() -> Unit): Task<I, O> {
        val builder = TaskBuilder<I, O>(name, O::class.java)
        builder.init()
        val task = builder.build()
        this.tasks[task.name] = task
        return task
    }

    fun <I : Any> fileImport(name: String, input: Publisher<I>, dockerDataDir: String?): FileImport<I> {
        val inFlux: Flux<I> = if (input is Flux) input else Flux.from(input)
        val fileImport = FileImport(name, inFlux, dockerDataDir ?: DEFAULT_DOCKER_DATA_DIR)
        this.fileImports[fileImport.name] = fileImport
        return fileImport
    }

    internal fun build(params: Params): Workflow {
        this.params = params
        this.init()
        return Workflow(name, tasks, fileImports)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
