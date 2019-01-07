package krews.executor

import krews.config.TaskConfig
import krews.core.TaskRunContext
import krews.file.InputFile
import krews.file.OutputFile
import java.nio.file.Path

const val RUN_DIR = "run"
const val OUTPUTS_DIR = "outputs"
const val DIAGNOSTICS_DIR = "diagnostics"
const val INPUTS_DIR = "inputs"
const val LOGS_DIR = "logs"
const val DB_FILENAME = "state/metadata.db"
const val REPORT_FILENAME = "status/report.html"


/**
 * Interface that deals with environment specific functionality when directing a workflow locally - from the current process,
 * ie. moving files around and running containers.
 */
interface LocallyDirectedExecutor {

    /**
     * Download the file at the given path
     */
    fun downloadFile(path: String)

    /**
     * Upload the given file
     */
    fun uploadFile(path: String)

    /**
     * @return True if the given file exists on the executor's file system, false otherwise.
     */
    fun fileExists(path: String): Boolean

    /**
     * @return The last modified date for the given path on the executor's file system.
     * Throws Exception if file does not exist
     */
    fun fileLastModified(path: String): Long

    /**
     * Execute task for the environment. Will consist of running a docker container to complete the task, and another
     * for downloading the given input files from remote sources, and possibly more for environment specific requirements.
     *
     * This function should block until the task is complete.
     *
     * @param outputFilesIn: Output files coming from a task's current "Input Element." These will already exist in the
     * current environment's storage.
     * @param outputFilesOut: Output files coming from a task's current "Output Element." These need to end up in the
     * current environment's storage.
     * @param cachedInputFiles: Input files that exist in the current environment's storage.
     * @param downloadInputFiles: Input files that need to be downloaded from original sources.
     */
    fun executeTask(workflowRunDir: String,
                    taskRunId: Int,
                    taskConfig: TaskConfig,
                    taskRunContext: TaskRunContext<*, *>,
                    outputFilesIn: Set<OutputFile>,
                    outputFilesOut: Set<OutputFile>,
                    cachedInputFiles: Set<InputFile>,
                    downloadInputFiles: Set<InputFile>)

    /**
     * Shuts down all tasks currently executing.
     * This function should NOT block if possible. This will run on SIGTERM. Fire-and-forget shutdown signalling is ok here.
     */
    fun shutdownRunningTasks()

    /**
     * Download input file from remote source and load it into /inputs directory.
     */
    fun downloadInputFile(inputFile: InputFile)

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
