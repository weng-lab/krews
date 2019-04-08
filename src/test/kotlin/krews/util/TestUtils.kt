package krews.util

import io.mockk.*
import krews.core.TaskRunContext
import krews.executor.LocallyDirectedExecutor
import mu.KotlinLogging
import java.nio.file.*

private val log = KotlinLogging.logger {}

/**
 * Recursively delete directory if it exists
 */
fun deleteDir(dir: Path) {
    if (Files.isDirectory(dir)) {
        Files.walk(dir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }
}

/**
 * Creates file with parent directories if they don't exist.
 * Adds given content to file
 */
fun createFile(file: Path, content: String) {
    Files.createDirectories(file.parent)
    Files.createFile(file)
    Files.write(file, content.toByteArray())
}

/**
 * Checks if an executor (spy) was called to execute with a given output file (as a path)
 *
 * @param times: the number of times it was called. Setting to 0 will verify it was not executed.
 */
fun verifyExecuteWithOutput(executorSpy: LocallyDirectedExecutor, path: String, times: Int = 1) {
    log.debug { "Verifying task execute for output file with path $path exactly $times times" }
    coVerify(exactly = times) {
        val executeTRMatch = match<List<TaskRunContext<*, *>>> {
            for (trc in it) {
                for (outputFile in trc.outputFilesOut) {
                    if (outputFile.path == path) {
                        return@match true
                    }
                }
            }
            return@match false
        }
        executorSpy.executeTask(any(), any(), any(), executeTRMatch)
    }
}

fun coEveryMatchTaskRun(executorSpy: LocallyDirectedExecutor, matchFn: (taskRunContext: TaskRunContext<*, *>) -> Boolean): MockKStubScope<Unit, Unit> {
    return coEvery {
        val dockerMatch = match<List<TaskRunContext<*, *>>> {
            for (tr in it) {
                if (matchFn(tr)) {
                    return@match true
                }
            }
            return@match false
        }
        executorSpy.executeTask(any(), any(), any(), dockerMatch)
    }
}