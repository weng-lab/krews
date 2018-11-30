package krews.core

import krews.config.convertConfigMap
import mu.KotlinLogging
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

val xlog = KotlinLogging.logger {}

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal val tasks: MutableMap<String, Task<*, *>> = mutableMapOf()
    private val fileImports: MutableMap<String, FileImport<*>> = mutableMapOf()
    @PublishedApi internal lateinit var rawParams: Map<String, Any>
    @PublishedApi internal var cachedParams: Any? = null

    inline fun <reified P> params(): P {
        if (cachedParams == null) {
            cachedParams = convertConfigMap<P>(rawParams)
        }
        return cachedParams as P
    }

    inline fun <reified I : Any, reified O : Any> task(name: String, init: TaskBuilder<I, O>.() -> Unit): Task<I, O> {
        val builder = TaskBuilder(name, I::class.java, O::class.java)
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

    internal fun build(rawParams: Map<String, Any>): Workflow {
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks, fileImports)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
