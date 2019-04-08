package krews.core

import krews.config.convertConfigMap
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal lateinit var tasks: MutableMap<String, Task<*, *>>
    @PublishedApi internal lateinit var rawParams: Map<String, Any>
    @PublishedApi internal var paramsOverride: Any? = null

    fun importWorkflow(workflowBuilder: WorkflowBuilder, params: Any? = null): Workflow {
        workflowBuilder.paramsOverride = params
        val imported = workflowBuilder.build(mapOf())
        this.tasks.putAll(imported.tasks)
        return imported
    }

    inline fun <reified P> params(): P = if (paramsOverride != null && paramsOverride is P) paramsOverride as P else convertConfigMap(rawParams)

    inline fun <reified I : Any, reified O : Any> task(name: String,
                                                       inputPub: Publisher<out I>,
                                                       vararg labels: String,
                                                       maintainOrder: Boolean = false,
                                                       noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Flux<O> {
        return this.task(name, inputPub, labels.toList(), maintainOrder, taskRunContextInit)
    }

    inline fun <reified I : Any, reified O : Any> task(name: String,
                                                       inputPub: Publisher<out I>,
                                                       labels: List<String> = listOf(),
                                                       maintainOrder: Boolean = false,
                                                       noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Flux<O> {
        val task = Task(name, inputPub, labels, I::class.java, O::class.java, maintainOrder, taskRunContextInit)
        if (this.tasks.containsKey(task.name)) {
            throw Exception("Task name ${task.name} used by more than one task. The same name cannot be used for two different tasks.")
        }
        this.tasks[task.name] = task
        return task.outputPub
    }

    internal fun build(rawParams: Map<String, Any>): Workflow {
        this.tasks = mutableMapOf()
        this.paramsOverride = null
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
