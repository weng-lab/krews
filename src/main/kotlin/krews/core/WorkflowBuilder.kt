package krews.core

import krews.config.convertConfigMap
import krews.executor.LocallyDirectedExecutor
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.nio.file.Path

class WorkflowBuilder internal constructor(val name: String, private val init: WorkflowBuilder.() -> Unit) {
    @PublishedApi internal lateinit var tasks: MutableMap<String, Task<*, *>>
    @PublishedApi internal lateinit var rawParams: Map<String, Any>
    @PublishedApi internal var paramsOverride: Any? = null
    private lateinit var executor: LocallyDirectedExecutor

    fun downloadFile(fromPath: String, toPath: Path) = executor.downloadFile(fromPath, toPath)
    fun uploadFile(fromPath: Path, toPath: String) = executor.uploadFile(fromPath, toPath)
    fun listFiles(baseDir: String) = executor.listFiles(baseDir)
    fun deleteFile(file: String) = executor.deleteFile(file)

    fun importWorkflow(workflowBuilder: WorkflowBuilder, executor: LocallyDirectedExecutor, params: Any? = null): Workflow {
        workflowBuilder.paramsOverride = params
        val imported = workflowBuilder.build(executor, mapOf())
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

    internal fun build(executor: LocallyDirectedExecutor, rawParams: Map<String, Any>): Workflow {
        this.tasks = mutableMapOf()
        this.paramsOverride = null
        this.rawParams = rawParams
        this.init()
        return Workflow(name, tasks)
    }
}

fun workflow(name: String, init: WorkflowBuilder.() -> Unit) = WorkflowBuilder(name, init)
