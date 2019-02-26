package krews.core

import krews.db.TaskRun
import krews.file.OutputFile
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

class TaskRunFuture<I : Any, O : Any>(
    val task: Task<I, O>,
    val taskRunContext: TaskRunContext<I, O>
) : CompletableFuture<O>() {
    var executedFuture: Future<Unit>? = null
    var taskRun: TaskRun? = null
    var outputFilesOut: Set<OutputFile>? = null
    var useCache: Boolean? = null
    var inputJson: String? = null
    var paramsJson: String? = null
}
