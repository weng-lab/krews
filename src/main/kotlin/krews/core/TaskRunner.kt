package krews.core

import krews.config.WorkflowConfig
import krews.db.*
import krews.executor.INPUTS_DIR
import krews.executor.LocallyDirectedExecutor
import krews.executor.OUTPUTS_DIR
import krews.executor.RUN_DIR
import krews.file.getInputFilesForObject
import krews.file.getOutputFilesForObject
import krews.misc.CacheView
import krews.misc.mapper
import mu.KotlinLogging
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Future


private val log = KotlinLogging.logger {}

class TaskRunner(private val workflowRun: WorkflowRun,
                 private val workflowConfig: WorkflowConfig,
                 private val executor: LocallyDirectedExecutor,
                 private val db: Database) {

    private val currentInputFileDownloads = ConcurrentHashMap<String, Future<*>>()
    private val inputDLPool = Executors.newCachedThreadPool(BasicThreadFactory.Builder().namingPattern("input-downloader-%d").build())
    private val workflowRunId = transaction(db) { workflowRun.id.value }
    private val workflowStartTime = transaction(db) { workflowRun.startTime }

    fun <I : Any, O : Any> run(task: Task<I, O>,
                               taskRunContext: TaskRunContext<I, O>): O {
        val taskConfig = workflowConfig.tasks[task.name]!!

        val inputJson = mapper
            .writerWithView(CacheView::class.java)
            .forType(task.inputClass)
            .writeValueAsString(taskRunContext.input)
        log.info {
            """
            |Running task "${task.name}" for dockerImage "${taskRunContext.dockerImage}"
            |Input: $inputJson
            |Command:
            |${taskRunContext.command}
            """.trimMargin()
        }

        val paramsJson =
            if (taskRunContext.taskParams != null) {
                mapper.writerWithView(CacheView::class.java)
                    .forType(taskRunContext.taskParamsClass)
                    .writeValueAsString(taskRunContext.taskParams)
            } else null

        val now = DateTime.now()
        val taskRun: TaskRun = transaction(db) {
            TaskRun.new {
                this.workflowRun = this@TaskRunner.workflowRun
                this.startTime = now.millis
                this.taskName = task.name
                this.inputJson = inputJson
                this.paramsJson = paramsJson
                this.command = taskRunContext.command
                this.image = taskRunContext.dockerImage
            }
        }

        val taskRunId: Int = transaction(db) { taskRun.id.value }
        log.info { "Task run created with id $taskRunId." }
        val loggingPrefix = "Task Run ${task.name} - $taskRunId:"
        val output = taskRunContext.output
        try {
            log.info { "$loggingPrefix checking for input files to download..." }

            // Download input files marked with "cache"
            val allInputFiles = getInputFilesForObject(taskRunContext.input) + getInputFilesForObject(taskRunContext.taskParams)
            for (inputFile in allInputFiles) {
                if (!inputFile.cache) continue

                val cachedInputFile = transaction(db) {
                    CachedInputFile
                        .find { CachedInputFiles.path eq inputFile.path }
                        .firstOrNull()
                }

                val cachedInputPath = "$INPUTS_DIR/${inputFile.path}"

                val needsDownload = cachedInputFile == null
                        || cachedInputFile.lastModifiedTime != inputFile.lastModified
                        || !executor.fileExists(cachedInputPath)
                        || cachedInputFile.cachedCopyLastModifiedTime != executor.fileLastModified(cachedInputPath)

                if (needsDownload) {
                    val dlFuture = currentInputFileDownloads.getOrPut(inputFile.path) {
                        log.info { "$loggingPrefix initiating download of input file $inputFile..." }
                        inputDLPool.submit {
                            executor.downloadInputFile(inputFile)
                            // Save changes. Delete any old entries for this input file path and create a new one.
                            transaction(db) {
                                CachedInputFiles.deleteWhere { CachedInputFiles.path eq inputFile.path }
                                CachedInputFile.new {
                                    path = inputFile.path
                                    lastModifiedTime = inputFile.lastModified
                                    cachedCopyLastModifiedTime = executor.fileLastModified(cachedInputPath)
                                    latestUseWorkflowRunId = workflowRunId
                                }
                            }
                        }
                    }
                    log.info { "$loggingPrefix waiting for download of input file $inputFile to complete..." }
                    dlFuture.get()
                } else {
                    log.info { "$loggingPrefix using cached copy of input file $inputFile" }
                    transaction(db) { cachedInputFile!!.latestUseWorkflowRunId = workflowRunId }
                }
            }

            // Check output cache to see if we can skip running this task.
            log.info {
                """
                |$loggingPrefix
                |Checking cache against
                |task-name = ${task.name}
                |input-json = $inputJson
                |params-json = $paramsJson
                |command = ${taskRunContext.command}
                """.trimMargin()
            }
            val cachedOutput: CachedOutput? = transaction(db) {
                CachedOutput.find {
                    CachedOutputs.taskName eq task.name and
                            (CachedOutputs.image eq taskRunContext.dockerImage) and
                            (CachedOutputs.inputJson eq inputJson) and
                            (CachedOutputs.paramsJson eq paramsJson) and
                            (CachedOutputs.command eq taskRunContext.command)
                }.firstOrNull()
            }

            var useCache = false
            if (cachedOutput != null) {
                useCache = true
                val cachedOutputFilePaths = cachedOutput.outputFiles.split(",")
                for (outputFilePath in cachedOutputFilePaths) {
                    if (!executor.fileExists("$OUTPUTS_DIR/$outputFilePath")) {
                        log.info { "$loggingPrefix Output file from cache $outputFilePath not found in output directory. Cache will not be used." }
                        transaction(db) { cachedOutput.delete() }
                        useCache = false
                        break
                    }
                }
            }

            val outputFilesOut = getOutputFilesForObject(output)

            if (useCache) {
                log.info { "$loggingPrefix Valid cached outputs found. Skipping execution." }
                transaction(db) {
                    taskRun.cacheUsed = true
                    cachedOutput!!.latestUseWorkflowRunId = workflowRunId
                }

            } else {
                log.info { "$loggingPrefix Valid cached outputs not found. Executing..." }
                val outputFilesIn = getOutputFilesForObject(taskRunContext.input)
                val cachedInputFiles = allInputFiles.filter { it.cache }.toSet()
                val downloadInputFiles = allInputFiles.filter { !it.cache }.toSet()
                executor.executeTask(
                    "$RUN_DIR/$workflowStartTime",
                    taskRunId,
                    taskConfig,
                    taskRunContext,
                    outputFilesIn,
                    outputFilesOut,
                    cachedInputFiles,
                    downloadInputFiles
                )
            }

            // Add last modified timestamps to output files in task output
            for (outputFile in outputFilesOut) {
                outputFile.lastModified = executor.fileLastModified("$OUTPUTS_DIR/${outputFile.path}")
            }

            if (!useCache) {
                val outputFilePaths = outputFilesOut.joinToString(",") { it.path }
                transaction(db) {
                    CachedOutput.new {
                        this.taskName = task.name
                        this.image = taskRunContext.dockerImage
                        this.inputJson = inputJson
                        this.paramsJson = paramsJson
                        this.command = taskRunContext.command
                        this.outputFiles = outputFilePaths
                        this.latestUseWorkflowRunId = workflowRunId
                    }
                }
            }

            val outputJson = mapper
                .writerWithView(CacheView::class.java)
                .forType(task.outputClass)
                .writeValueAsString(output)

            log.info { "$loggingPrefix Task completed successfully! Saving status..." }
            transaction(db) {
                taskRun.outputJson = outputJson
                taskRun.completedSuccessfully = true
                taskRun.completedTime = DateTime.now().millis
                log.info { "Task status save complete." }
            }
        } catch (e: Exception) {
            transaction(db) { taskRun.completedTime = DateTime.now().millis }
            throw e
        }
        return output
    }

}