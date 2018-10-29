package krews

import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor

class Task<I : Any, O : Any> internal constructor(
        val workflow: Workflow,
        val name: String,
        val labels: List<String> = listOf(),
        val input: Flux<I>,
        val docker: TaskDocker,
        val requirements: TaskRequirements?,
        private val outputFn: (inputItem: I) -> O,
        private val scriptFn: (inputItem: I) -> String,
        internal val outputClass: Class<O>) {

    val output: Flux<O> = TopicProcessor.create<O>()

    internal lateinit var executeFn: (script: String, inputItem: Any, outputItem: Any?) -> Unit

    internal fun connect() {
        val processed = input.map { processInput(it) }
        processed.subscribe(output as TopicProcessor<O>)
    }

    private fun processInput(inputItem: I): O {
        val outputItem = outputFn(inputItem)
        val script = scriptFn(inputItem)
        executeFn(script, inputItem, outputItem)
        return outputItem
    }
}

data class TaskDocker(val image: String, val dataDir: String)

data class TaskRequirements(val cpus: Int? = null, val mem: Capacity? = null, val disk: Capacity? = null)

enum class CapacityType(val bytesMultiplier: Long) {
    B(1),
    KB(1024),
    MB(KB.bytesMultiplier*1024),
    GB(MB.bytesMultiplier*1024),
    TB(GB.bytesMultiplier*1024)
}

data class Capacity(val value: Long, val type: CapacityType) {
    val bytes: Long get() = value * type.bytesMultiplier
}

internal fun String.toCapacity(): Capacity {
    val regex = """(\d+)\s*([KMGT]?B)""".toRegex()
    val matchResult = regex.find(this)
    val (value, type) = matchResult!!.destructured
    return Capacity(value.toLong(), CapacityType.valueOf(type))
}

inline fun <I : Any, reified O : Any> task(name: String, noinline init: TaskBuilder<I, O>.() -> Unit): Task<I, O> = defaultWorkflow.task(name, init)