package krews.core

import krews.config.convertConfigMap
import org.reactivestreams.Publisher

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal val tasks: MutableSet<Task<*, *>> = mutableSetOf()
    @PublishedApi internal lateinit var rawParams: Map<String, Any>
    @PublishedApi internal var paramsOverride: Any? = null
    @PublishedApi internal val taskNameSeen: MutableMap<String, Boolean> = mutableMapOf()

    fun importWorkflow(workflowBuilder: WorkflowBuilder, params: Any? = null): Workflow {
        workflowBuilder.paramsOverride = params
        val imported = workflowBuilder.build(mapOf())
        this.tasks.addAll(imported.tasks)
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
        // The assumption here is that tasks with the same name, created in the same order are the same
        // Technically, we can assign an id of whatever we want, it only has to be unique
        // Only using the name may result in conflicts when the following occurs
        // 1) The user creates two completely different tasks with the same name. This is usually unintended.
        // 2) The user creates the same tasks multiple times (i.e. calls task(...) multiple times in a workflow).
        //    This is may be intended when a single task needs to be done in different steps of the workflow.
        // 3) The user creates the same task multiple times with a different closed environment.
        //    This is almost almost always intended.
        //
        // For case 1, ideally we would error immediately, since logs could be confusing.
        // For case 2, this should be fine. Having a different name for these may just introduce extra boilerplate.
        // For case 3, it's unclear. Having different names makes having different configs straightforward, but may lead
        // to extra boilerplate.
        //
        // The most correct ways to handle duplicate names is either to error immediately or fallback to a unique id
        // Here, we error immediately and stop execution.
        val seen = taskNameSeen.putIfAbsent(name, true)
        if (seen == true) {
            System.err.println("Task '$name' is duplicated. If you want to use it twice, call it with different names.")
            System.exit(1)
        }
        val task = Task( name, inputPub, labels, I::class.java, O::class.java, maintainOrder, taskRunContextInit)
        tasks.add(task)
        return task
    }

    internal fun build(rawParams: Map<String, Any>): Workflow {
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
