package krews.core

import krews.db.*
import krews.executor.LocallyDirectedExecutor
import krews.file.getOutputFilesForObject
import krews.misc.*
import mu.KotlinLogging
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

private val log = KotlinLogging.logger {}

class Cache(private val cacheDb: Database, private val executor: LocallyDirectedExecutor) {

    /**
     * Checks the cache for the given TaskRunContext.
     *
     * @returns true if the run was cached AND if the outputs from the run still exist.
     */
    fun checkCache(taskRunContext: TaskRunContext<*, *>, workflowRunId: Int): Boolean {
        val inputJson = inputJson(taskRunContext.input, taskRunContext.inputClass)
        val paramsJson = paramsJson(taskRunContext.taskParams, taskRunContext.taskParamsClass)

        // Check output cache to see if we can skip running this task.
        log.info {
            """
            |Checking cache against
            |task-name = ${taskRunContext.taskName}
            |input-json = $inputJson
            |params-json = $paramsJson
            |command = ${taskRunContext.command}
            """.trimMargin()
        }

        // Fetch the previous cache
        val cachedTaskRunExecution: CachedTaskRunExecution = transaction(cacheDb) {
            CachedTaskRunExecution.find {
                CachedTaskRunExecutions.taskName eq taskRunContext.taskName and
                        (CachedTaskRunExecutions.image eq taskRunContext.dockerImage) and
                        (CachedTaskRunExecutions.inputJson eq inputJson) and
                        (CachedTaskRunExecutions.paramsJson eq paramsJson) and
                        (CachedTaskRunExecutions.command eq taskRunContext.command)
            }.firstOrNull()
        } ?: return false

        // Make sure the output files from the cached task run execution still exist.
        val cachedOutputFilePaths = cachedTaskRunExecution.outputFiles.split(",")
        for (outputFilePath in cachedOutputFilePaths) {
            if (!executor.fileExists("$OUTPUTS_DIR/$outputFilePath")) {
                log.info { "Output file from cache $outputFilePath not found in output directory. Cache will not be used." }
                transaction(cacheDb) { cachedTaskRunExecution.delete() }
                return false
            }
        }

        // If the cache entry exists and we're using it, set it's workflowRunId to this one.
        transaction(cacheDb) {
            cachedTaskRunExecution.latestUseWorkflowRunId = workflowRunId
        }
        return true
    }

    /**
     * Create a new entry in the cache for this taskRunContext
     */
    fun updateCache(taskRunContext: TaskRunContext<*, *>, workflowRunId: Int) {
        val outputFilesOut = getOutputFilesForObject(taskRunContext.output)
        val outputFilePaths = outputFilesOut.joinToString(",") { it.path }
        val inputJson = inputJson(taskRunContext.input, taskRunContext.inputClass)
        val paramsJson = paramsJson(taskRunContext.taskParams, taskRunContext.taskParamsClass)
        transaction(cacheDb) {
            CachedTaskRunExecution.new {
                this.taskName = taskRunContext.taskName
                this.image = taskRunContext.dockerImage
                this.inputJson = inputJson
                this.paramsJson = paramsJson
                this.command = taskRunContext.command
                this.outputFiles = outputFilePaths
                this.latestUseWorkflowRunId = workflowRunId
            }
        }
    }

}

private fun inputJson(input: Any, inputClass: Class<*>) =
    mapper.writer().forType(inputClass).writeValueAsString(input)

private fun paramsJson(taskParams: Any?, taskParamsClass: Class<*>?) =
    if (taskParams != null) {
        mapper.writer().forType(taskParamsClass).writeValueAsString(taskParams)
    } else null