package krews.core

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class Workflow internal constructor(
    val name: String,
    internal val tasks: Map<String, Task<*, *>>,
    internal val fileImports: Map<String, FileImport<*>>
) {
    @Suppress("UNCHECKED_CAST")
    fun <I> taskInputPub(taskName: String) = tasks[taskName]!!.inputPub as Publisher<I>

    @Suppress("UNCHECKED_CAST")
    fun <O> taskOutputPub(taskName: String) = tasks[taskName]!!.outputPub as Flux<O>

    @Suppress("UNCHECKED_CAST")
    fun <O> fileImportOutputPub(fileImportName: String) = fileImports[fileImportName]!!.output as Flux<O>
}