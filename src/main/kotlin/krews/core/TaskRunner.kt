package krews.core

import com.fasterxml.jackson.core.type.TypeReference
import krews.config.*
import krews.db.*
import krews.executor.LocallyDirectedExecutor
import krews.misc.mapper
import mu.KotlinLogging
import java.util.concurrent.*
import kotlinx.coroutines.*
import krews.file.NONE_SUFFIX
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger


private val log = KotlinLogging.logger {}

class TaskRunFuture<I : Any, O : Any>(
    val taskRunContext: TaskRunContext<I, O>
): CompletableFuture<O>() {
    fun complete() {
        complete(taskRunContext.output)
    }
}

class TaskRunner(workflowRun: WorkflowRun,
                 private val workflowConfig: WorkflowConfig,
                 private val taskConfigs: Map<String, TaskConfig>,
                 private val executor: LocallyDirectedExecutor,
                 private val runRepo: RunRepo) {

    private val workflowStartTime = runRepo.workflowStartTime(workflowRun)

    private val stopped = AtomicBoolean(false)
    private val coroutineExecutor = Executors.newFixedThreadPool(workflowConfig.executorConcurrency)
    private val coroutineDispatcher = coroutineExecutor.asCoroutineDispatcher()

    private val taskRunThreadFactory = BasicThreadFactory.Builder().namingPattern("task-run-%d").build()
    private val taskRunPool = Executors.newSingleThreadScheduledExecutor(taskRunThreadFactory)

    private val groupingBuffers: MutableMap<String, MutableList<TaskRunFuture<*, *>>> = Collections.synchronizedMap(mutableMapOf())
    private val queue: LinkedBlockingQueue<List<TaskRunFuture<*, *>>> =  LinkedBlockingQueue()
    private val running: AtomicInteger = AtomicInteger(0)

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
            if (workflowConfig.parallelism is LimitedParallelism) workflowConfig.parallelism.limit - running.get()
            else Integer.MAX_VALUE - running.get()

        while (openSlots > 0) {
            openSlots--
            // This MUST be a queue. The queue may be modified (in submit) concurrently,
            // so it needs to be thread-safe, and cannot be iterated over.
            val queued = queue.poll() ?: break
            GlobalScope.launch (coroutineDispatcher) {
                running.incrementAndGet()
                this@TaskRunner.run(queued)
                running.decrementAndGet()
            }
        }
    }

    /**
     * Submit a TaskRunContext.
     *
     * If all the output files already exist in the executor's file storage, the taskRunContext will skip and complete immediately.
     * Otherwise the taskRunContext will be added to a grouping buffer for the task. If the buffer is over a
     * configured grouping threshold, the group will be queued.
     */
    fun <I : Any, O : Any> submit(taskRunContext: TaskRunContext<I, O>): CompletableFuture<O> = synchronized(this) {
        val taskName = taskRunContext.taskName
        groupingBuffers.putIfAbsent(taskName, Collections.synchronizedList(mutableListOf()))

        val taskRunFuture = TaskRunFuture(taskRunContext)

        // Check if all the output files exist and we should skip this run
        val allOutputFilesPresent = taskRunContext.outputFilesOut.isNotEmpty() && taskRunContext.outputFilesOut.all {
            executor.fileExists("$OUTPUTS_DIR/${it.path}") ||
                    (it.optional && executor.fileExists("$OUTPUTS_DIR/${it.path}.$NONE_SUFFIX"))
        }
        when {
            workflowConfig.forceRuns -> log.info { "ForceRuns config set to true. Queueing...\n$taskRunContext" }
            allOutputFilesPresent -> {
                log.info { "All output files found for the following execution. Skipping...\n$taskRunContext" }
                taskRunFuture.complete()
                return taskRunFuture
            }
            else -> log.info { "Some output files not found. Queueing...\n$taskRunContext" }
        }

        val taskGroupingConfig = if (executor.supportsGrouping()) {
            taskConfigs.getValue(taskName).grouping
        } else 1
        val taskGroupingBuffer = groupingBuffers.getValue(taskName)
        taskGroupingBuffer += taskRunFuture
        log.debug { "Task $taskName grouping buffer size currently ${taskGroupingBuffer.size}. " +
                "Grouping config set to $taskGroupingConfig." }
        if (taskGroupingBuffer.size >= taskGroupingConfig) {
            log.debug { "Task grouping buffer for $taskName full. Queueing..." }
            val taskBufferCopy = taskGroupingBuffer.map { it }
            try {
                queue.put(taskBufferCopy)
            } catch (e: Exception) {
                log.error { "Unexpected error: $e" }
            }
            taskGroupingBuffer.clear()
        }
        return taskRunFuture
    }

    /**
     * Notify the TaskRunner that we have received all taskRunContexts for a task, and therefor we should
     * queue any groupings that may not be full.
     */
    fun taskComplete(taskName: String) = synchronized(this) {
        // TODO: how is this called before submit (when task fails)
        if (!groupingBuffers.containsKey(taskName)) {
            return
        }
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
    private suspend fun run(taskRunFutures: List<TaskRunFuture<*, *>>) {
        val taskRunContexts = taskRunFutures.map { it.taskRunContext }
        val taskName = taskRunContexts.first().taskName
        val taskConfig = taskConfigs.getValue(taskName)

        val taskRunExecutions = taskRunContexts
            .map { TaskRunExecution(it.dockerImage, it.command, it.input, it.output) }

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

            var allContextsSuccessful = true
            taskRunFutures.forEach { taskRunFuture ->
                val taskRunContext = taskRunFuture.taskRunContext
                val allOutputsExist = taskRunContext.outputFilesOut.all {
                    executor.fileExists("$OUTPUTS_DIR/${it.path}") ||
                            (it.optional && executor.fileExists("$OUTPUTS_DIR/${it.path}.$NONE_SUFFIX"))
                }
                if (allOutputsExist) {
                    taskRunFuture.complete()
                } else {
                    taskRunFuture.completeExceptionally(Exception("All output files were not created."))
                    allContextsSuccessful = false
                }
            }

            if (allContextsSuccessful) {
                log.info { "$loggingPrefix Task completed successfully! Saving status..." }
                runRepo.completeTaskRun(taskRun, completed = "completed")
            } else {
                log.info { "$loggingPrefix Task completed partially successful! Saving status..." }
                runRepo.completeTaskRun(taskRun, completed = "partially completed")
            }

            log.info { "Task status save complete." }
        } catch (e: Exception) {
            log.info { "$loggingPrefix Task failed! Saving status..." }
            runRepo.completeTaskRun(taskRun, completed = "failed")

            taskRunFutures.forEach {
                it.completeExceptionally(e)
            }
        }
    }

}