package krews.core

import krews.config.convertConfigMap

class Workflow internal constructor(
    val name: String,
    internal val tasks: Set<Task<*, *>>
)

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal val tasks: MutableSet<Task<*, *>> = mutableSetOf()
    @PublishedApi internal lateinit var rawParams: Map<String, Any>
    @PublishedApi internal var paramsOverride: Any? = null

    fun importWorkflow(workflowBuilder: WorkflowBuilder, params: Any? = null): Workflow {
        workflowBuilder.paramsOverride = params
        val imported = workflowBuilder.build(mapOf())
        this.tasks += imported.tasks
        return imported
    }

    inline fun <reified P> params(): P = if (paramsOverride != null && paramsOverride is P) paramsOverride as P else convertConfigMap(rawParams)

    inline fun <reified I : Any, reified O : Any> task(
            name: String,
            inputs: List<I>,
            vararg labels: String,
            noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Task<I, O> {
        return this.task(name, inputs, labels.toList(), taskRunContextInit)
    }

    inline fun <reified I : Any, reified O : Any> task(
            name: String,
            inputs: List<I>,
            labels: List<String> = listOf(),
            noinline taskRunContextInit: TaskRunContextBuilder<I, O>.() -> Unit): Task<I, O> {
        val task = Task(name, inputs, labels, taskRunContextInit)
        this.tasks += task
        return task
    }

    internal fun build(rawParams: Map<String, Any>): Workflow {
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)