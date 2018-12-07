package krews.core

import krews.config.convertConfigMap
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal val tasks: MutableMap<String, Task<*, *>> = mutableMapOf()
    private val fileImports: MutableMap<String, FileImport<*>> = mutableMapOf()
    @PublishedApi internal lateinit var rawParams: Map<String, Any>
    @PublishedApi internal var params: Any? = null

    inline fun <reified P> params(): P {
        if (params == null || params !is P) {
            params = convertConfigMap<P>(rawParams)
        }
        return params as P
    }

    inline fun <reified I : Any, reified O : Any> task(name: String,
                                                       inputPub: Publisher<out I>,
                                                       vararg labels: String,
                                                       noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Task<I, O> {
        return this.task(name, inputPub, labels.toList(), taskRunContextInit)
    }

    inline fun <reified I : Any, reified O : Any> task(name: String,
                                                       inputPub: Publisher<out I>,
                                                       labels: List<String> = listOf(),
                                                       noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Task<I, O> {
        val task = Task(name, inputPub, labels, I::class.java, O::class.java, taskRunContextInit)
        this.tasks[task.name] = task
        return task
    }

    fun <I : Any> fileImport(name: String, input: Publisher<I>, dockerDataDir: String?): FileImport<I> {
        val inFlux: Flux<I> = if (input is Flux) input else Flux.from(input)
        val fileImport = FileImport(name, inFlux, dockerDataDir ?: DEFAULT_DOCKER_DATA_DIR)
        this.fileImports[fileImport.name] = fileImport
        return fileImport
    }

    internal fun build(rawParams: Map<String, Any>): Workflow {
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks, fileImports)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
