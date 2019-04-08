package krews.core

import com.fasterxml.jackson.core.type.TypeReference
import krews.config.*
import krews.db.*
import krews.executor.LocallyDirectedExecutor
import krews.misc.mapper
import mu.KotlinLogging
import org.jetbrains.exposed.sql.*
import java.util.concurrent.*
import kotlinx.coroutines.*
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean


private val log = KotlinLogging.logger {}

class TaskRunFuture<I : Any, O : Any>(
    val taskRunContext: TaskRunContext<I, O>,
    private val executor: LocallyDirectedExecutor
): CompletableFuture<O>() {
    fun complete() {
        // Add last modified timestamps to output files in task output
        taskRunContext.outputFilesOut.forEach { file ->
            file.lastModified = executor.fileLastModified("$OUTPUTS_DIR/${file.path}")
        }
        complete(taskRunContext.output)
    }
}

class TaskRunner(workflowRun: WorkflowRun,
                 private val workflowConfig: WorkflowConfig,
                 private val executor: LocallyDirectedExecutor,
                 private val runRepo: RunRepo,
                 cacheDb: Database) {

    private val workflowRunId = workflowRun.id.value
    private val workflowStartTime = runRepo.workflowStartTime(workflowRun)
    private val cache = Cache(cacheDb, executor)

    private val stopped = AtomicBoolean(false)
    private val coroutineExecutor = Executors.newFixedThreadPool(workflowConfig.executorConcurrency)
    private val coroutineDispatcher = coroutineExecutor.asCoroutineDispatcher()

    private val taskRunThreadFactory = BasicThreadFactory.Builder().namingPattern("task-run-%d").build()
    private val taskRunPool = Executors.newSingleThreadScheduledExecutor(taskRunThreadFactory)

    private val groupingBuffers: MutableMap<String, MutableList<TaskRunFuture<*, *>>> = Collections.synchronizedMap(mutableMapOf())
    private val queue: MutableList<List<TaskRunFuture<*, *>>> = Collections.synchronizedList(mutableListOf())
    private val running: MutableList<List<TaskRunFuture<*, *>>> = Collections.synchronizedList(mutableListOf())

    init {
        taskRunPool.scheduleWithFixedDelay({ checkQueue() }, 1, 1, TimeUnit.SECONDS)
    }

    fun stop() {
        stopped.set(true)
        taskRunPool.shutdown()
        taskRunPool.awaitTermination(1, TimeUnit.SECONDS)
        coroutineExecutor.shutdown()
        coroutineExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS)
    }

    /**
     * Check queue and if parallelism allows for more running tasks, launch task runs
     */
    private fun checkQueue() {
        if (stopped.get()) return

        var openSlots =
            if (workflowConfig.parallelism is LimitedParallelism) {
                workflowConfig.parallelism.limit - running.size
            } else null
        val queueIter = queue.iterator()
        queueIter.forEach { queued ->
            if (openSlots == 0) return@forEach
            if (openSlots != null) openSlots--
            queueIter.remove()
            GlobalScope.launch (coroutineDispatcher) {
                try {
                    this@TaskRunner.run(queued.map { it.taskRunContext })
                    queued.forEach {
                        cache.updateCache(it.taskRunContext, workflowRunId)
                        it.complete()
                    }
                } catch (e: Throwable) {
                    queued.forEach { it.completeExceptionally(e) }
                }
            }
        }
    }

    /**
     * Submit a TaskRunContext.
     *
     * The cache will be checked, if the cache is used the taskRunContext will skip and complete immediately.
     * Otherwise the taskRunContext will be added to a grouping buffer for the task. If the buffer is over a
     * configured grouping threshold, the group will be queued.
     */
    fun <I : Any, O : Any> submit(taskRunContext: TaskRunContext<I, O>): CompletableFuture<O> = synchronized(this) {
        log.info { "HERE: In Submit - $taskRunContext" }
        val taskName = taskRunContext.taskName
        groupingBuffers.putIfAbsent(taskName, Collections.synchronizedList(mutableListOf()))

        val taskRunFuture = TaskRunFuture(taskRunContext, executor)
        // Check the cache
        val useCache = cache.checkCache(taskRunContext, workflowRunId)
        if (useCache) {
            log.info { "Valid cached execution found for the following. Skipping execution.\n$taskRunContext" }
            taskRunFuture.complete()
            return taskRunFuture
        } else {
            log.info { "Cache not used for the following. Queueing...\n$taskRunContext" }
        }

        val taskGroupingConfig = if (executor.supportsGrouping()) {
            workflowConfig.tasks.getValue(taskName).grouping
        } else 1
        val taskGroupingBuffer = groupingBuffers.getValue(taskName)
        taskGroupingBuffer += taskRunFuture
        log.debug { "Task $taskName grouping buffer size currently ${taskGroupingBuffer.size}. " +
                "Grouping config set to $taskGroupingConfig." }
        if (taskGroupingBuffer.size >= taskGroupingConfig) {
            log.debug { "Task grouping buffer for $taskName full. Queueing..." }
            val taskBufferCopy = taskGroupingBuffer.map { it }
            queue += taskBufferCopy
            taskGroupingBuffer.clear()
        }
        return taskRunFuture
    }

    /**
     * Notify the TaskRunner that we have received all taskRunContexts for a task, and therefor we should
     * queue any groupings that may not be full.
     */
    fun taskComplete(taskName: String) = synchronized(this) {
        log.info { "HERE: IN TASK COMPLETE" }

        val taskGroupingBuffer = groupingBuffers.getValue(taskName)
        if (taskGroupingBuffer.size > 0) {
            val taskBufferCopy = taskGroupingBuffer.map { it }
            queue += taskBufferCopy
            taskGroupingBuffer.clear()
        }
    }

    /**
     * Run a group of TaskRunContexts as a single executor job.
     */
    private suspend fun run(taskRunContexts: List<TaskRunContext<*, *>>) {
        val taskName = taskRunContexts.first().taskName
        val taskConfig = workflowConfig.tasks.getValue(taskName)

        val taskRunExecutions = taskRunContexts
            .map { TaskRunExecution(it.dockerImage, it.command) }

        val taskRunExecutionsJson = mapper
            .writer()
            .forType(object : TypeReference<List<TaskRunExecution>>(){})
            .writeValueAsString(taskRunExecutions)

        val taskRun = runRepo.createTaskRun(taskName, taskRunExecutionsJson)
        val taskRunId = taskRun.id.value

        log.info { "Task run created with id $taskRunId." }
        val loggingPrefix = "Task Run $taskName - $taskRunId:"
        try {
            executor.executeTask(
                "$RUN_DIR/$workflowStartTime",
                taskRunId,
                taskConfig,
                taskRunContexts
            )

            log.info { "$loggingPrefix Task completed successfully! Saving status..." }
            runRepo.completeTaskRun(taskRun, successful = true)
            log.info { "Task status save complete." }
        } catch (e: Exception) {
            runRepo.completeTaskRun(taskRun, successful = false)
            throw e
        }
    }

}