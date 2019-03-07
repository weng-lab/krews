package krews

import io.mockk.coVerify
import io.mockk.verify
import krews.executor.LocallyDirectedExecutor
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Checks if the executor downloaded an input file into the /input cache directory.
 *
 * @param times: the number of times it was called. Setting to 0 will verify it was not executed.
 */
fun verifyInputFileCached(executorSpy: LocallyDirectedExecutor, path: String, times: Int = 1) {
    log.debug { "Verifying task downloaded input cache file with path $path exactly $times times" }
    verify(exactly = times) {
        executorSpy.downloadInputFile(match { it.path == path })
    }
}

/**
 * Checks if an executor (spy) was called to execute with a given output file (as a path)
 *
 * @param times: the number of times it was called. Setting to 0 will verify it was not executed.
 */
fun verifyExecuteWithOutput(executorSpy: LocallyDirectedExecutor, path: String, times: Int = 1) {
    log.debug { "Verifying task execute for output file with path $path exactly $times times" }
    coVerify(exactly = times) {
        executorSpy.executeTask(any(), any(), any(), any(), any(),
            match { if (it.isEmpty()) false else it.iterator().next().path == path },
            any(), any())
    }
}