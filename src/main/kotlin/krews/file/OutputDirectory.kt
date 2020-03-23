package krews.file

import java.util.concurrent.CompletableFuture
import reactor.core.publisher.*

/**
 * Krews' representation of a directory Files that results from running a task.
 */
data class OutputDirectory(val path: String) {
    internal val filesFuture: CompletableFuture<List<OutputFile>> = CompletableFuture()
    val files: Mono<List<OutputFile>> = Mono.fromFuture(filesFuture)
}
