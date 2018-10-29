package krews

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class TaskBuilder<I : Any, O : Any> @PublishedApi internal constructor(
    private val workflow: Workflow,
    private val name: String,
    private val outputClass: Class<O>) {
    var docker: TaskDocker? = null
        private set
    var requirements: TaskRequirements? = null
        private set
    var input: Publisher<I>? = null
    private var outputFn: ((inputItem: I) -> O)? = null
    private var scriptFn: ((inputItem: I) -> String)? = null
    var labels: List<String> = listOf()

    fun docker(init: TaskDockerBuilder.() -> Unit) {
        val taskDockerBuilder = TaskDockerBuilder()
        taskDockerBuilder.init()
        docker = taskDockerBuilder.build()
    }

    fun requirements(init: TaskRequirementsBuilder.() -> Unit) {
        val taskRequirementsBuilder = TaskRequirementsBuilder()
        taskRequirementsBuilder.init()
        requirements = taskRequirementsBuilder.build()
    }

    fun outputFn(init: InputItemContext<I>.() -> O) {
        outputFn = { inputItem ->
            InputItemContext(inputItem).init()
        }
    }

    fun scriptFn(init: InputItemContext<I>.() -> String) {
        scriptFn = { inputItem ->
            InputItemContext(inputItem).init()
        }
    }

    @PublishedApi internal fun build(): Task<I, O> {
        val inputNN: Publisher<I> = checkNotNull(input)
        val inFlux: Flux<I> = if (inputNN is Flux) inputNN else Flux.from(inputNN)
        return Task(
            workflow = workflow,
            name = name,
            labels = this.labels,
            input = inFlux,
            docker = checkNotNull(docker),
            requirements = requirements,
            outputFn = checkNotNull(outputFn),
            scriptFn = checkNotNull(scriptFn),
            outputClass = outputClass)
    }
}

data class InputItemContext<I : Any>(val inputItem: I)

class TaskDockerBuilder {
    var image: String? = null
    var dataDir: String? = null

    fun build() = TaskDocker(
        image = checkNotNull(image),
        dataDir = checkNotNull(dataDir))
}

class TaskRequirementsBuilder {
    var cpus: Int? = null
    var mem: String? = null
    var disk: String? = null

    fun build() = TaskRequirements(
        cpus = cpus,
        mem = mem?.toCapacity(),
        disk = disk?.toCapacity())
}