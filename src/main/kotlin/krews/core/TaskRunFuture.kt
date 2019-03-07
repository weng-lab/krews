package krews.core

import krews.db.TaskRun
import krews.file.OutputFile
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

class TaskRunFuture<I : Any, O : Any>(
    val task: Task<I, O>,
    val taskRunContext: TaskRunContext<I, O>
) : CompletableFuture<O>() {
    fun complete() {
        complete(taskRunContext.output)
    }
}
