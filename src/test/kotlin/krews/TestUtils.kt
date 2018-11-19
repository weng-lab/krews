package krews

import io.mockk.verify
import krews.executor.LocallyDirectedExecutor
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Checks if an executor (spy) was called to copy the given cached input file
 */
fun verifyCachedInputFile(executorSpy: LocallyDirectedExecutor, path: String) {
    log.debug { "Verifying cache copy for input file with path $path" }
    verify {
        executorSpy.copyCachedFiles(any(), any(),
            match { if (it.isEmpty()) false else it.iterator().next() == path })
    }
}

/**
 * Checks if an executor (spy) was called to execute with an input file (as a path) to download
 */
fun verifyDownloadInputFile(executorSpy: LocallyDirectedExecutor, path: String) {
    log.debug { "Verifying download for input file with path $path" }
    verify {
        executorSpy.executeTask(any(), any(), any(), any(), any(), any(), any(), any(), setOf(),
            match { if (it.isEmpty()) false else it.iterator().next().path == path })
    }
}

/**
 * Checks if an executor (spy) was called to execute with a given output file (as a path)
 *
 * @param times: the number of times it was called. Setting to 0 will verify it was not executed.
 */
fun verifyExecuteWithOutput(executorSpy: LocallyDirectedExecutor, path: String, times: Int = 1) {
    log.debug { "Verifying task execute for output file with path $path exactly $times times" }
    verify(exactly = times) {
        executorSpy.executeTask(any(), any(), any(), any(), any(), any(), any(),
            match { if (it.isEmpty()) false else it.iterator().next().path == path },
            any(), any())
    }
}