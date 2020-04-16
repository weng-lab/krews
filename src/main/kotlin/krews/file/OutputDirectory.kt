package krews.file

import com.fasterxml.jackson.annotation.JsonIgnore
import java.util.concurrent.CompletableFuture
import reactor.core.publisher.*

/**
 * Krews' representation of a directory Files that results from running a task.
 */
data class OutputDirectory(val path: String) {
    @get:JsonIgnore
    internal val filesFuture: CompletableFuture<List<OutputFile>> = CompletableFuture()
    @get:JsonIgnore
    val files: Mono<List<OutputFile>> = Mono.fromFuture(filesFuture)
}
