package krews.executor

import krews.config.TaskConfig
import krews.core.TaskRunContext
import java.nio.file.Path

/**
 * Interface that deals with environment specific functionality when directing a workflow locally - from the current process,
 * ie. moving files around and running containers.
 */
interface LocallyDirectedExecutor {

    fun supportsGrouping() = true

    /**
     * Download the file at the given path
     */
    fun downloadFile(fromPath: String, toPath: Path)

    /**
     * Upload the given file
     * @param backup: If a file exists at toPath already back it up after copying over it.
     */
    fun uploadFile(fromPath: Path, toPath: String, backup: Boolean = false)

    /**
     * @return True if the given file exists on the executor's file system, false otherwise.
     */
    fun fileExists(path: String): Boolean

    /**
     * Execute task for the environment. Will consist of running a docker container to complete the task, and another
     * for downloading the given input files from remote sources, and possibly more for environment specific requirements.
     *
     * This function should returns when the task is complete.
     */
    suspend fun executeTask(workflowRunDir: String,
                    taskRunId: Int,
                    taskConfig: TaskConfig,
                    taskRunContexts: List<TaskRunContext<*, *>>)

    /**
     * Shuts down all tasks currently executing.
     * This function should NOT block if possible. This will run on SIGTERM. Fire-and-forget shutdown signalling is ok here.
     */
    fun shutdownRunningTasks()

    fun listFiles(baseDir: String): Set<String>

    /**
     * Deletes the given file. Used for cleaning up unused files.
     */
    fun deleteFile(file: String)
}

/**
 * Interface that deals with environment specific functionality when directing a workflow remotely.
 * This will involve shipping the current executable off somewhere else where it will run with a LocallyDirectedExecutor
 */
interface RemoteDirectedExecutor {
    fun execute(executablePath: Path, configPath: Path)
}
