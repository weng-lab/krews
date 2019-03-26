package krews.core

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

class Workflow internal constructor(
    val name: String,
    internal val tasks: Set<Task<*, *>>
) {}