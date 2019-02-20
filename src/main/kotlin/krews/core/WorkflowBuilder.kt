package krews.core

import krews.config.convertConfigMap
import org.reactivestreams.Publisher

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal val tasks: MutableMap<String, Task<*, *>> = mutableMapOf()
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
                                                       noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Task<I, O> {
        return this.task(name, inputPub, labels.toList(), maintainOrder, taskRunContextInit)
    }

    inline fun <reified I : Any, reified O : Any> task(name: String,
                                                       inputPub: Publisher<out I>,
                                                       labels: List<String> = listOf(),
                                                       maintainOrder: Boolean = false,
                                                       noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Task<I, O> {
        val task = Task(name, inputPub, labels, I::class.java, O::class.java, maintainOrder, taskRunContextInit)
        this.tasks[task.name] = task
        return task
    }

    internal fun build(rawParams: Map<String, Any>): Workflow {
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
