package krews.core

import reactor.core.publisher.Flux

class Workflow internal constructor(
    val name: String,
    internal val tasks: Map<String, Task<*, *>>,
    internal val fileImports: Map<String, FileImport<*>>
) {
    @Suppress("UNCHECKED_CAST")
    fun <I> taskInput(taskName: String) = tasks[taskName]!!.input as Flux<I>

    @Suppress("UNCHECKED_CAST")
    fun <O> taskOutput(taskName: String) = tasks[taskName]!!.output as Flux<O>

    @Suppress("UNCHECKED_CAST")
    fun <O> fileImportOutput(fileImportName: String) = fileImports[fileImportName]!!.output as Flux<O>
}