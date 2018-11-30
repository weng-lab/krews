package krews.executor

import krews.config.TaskConfig
import krews.file.InputFile
import krews.file.OutputFile
import java.nio.file.Path

const val RUN_DIR = "run"
const val OUTPUTS_DIR = "outputs"
const val INPUTS_DIR = "inputs"
const val LOGS_DIR = "logs"
const val STATE_DIR = "state"
const val DB_FILENAME = "metadata.db"


/**
 * Interface that deals with environment specific functionality when directing a workflow locally - from the current process,
 * ie. moving files around and running containers.
 */
interface LocallyDirectedExecutor {

    /**
     * Download the database file if remote and return path
     */
    fun prepareDatabaseFile(): String

    /**
     * Upload the database file if remote
     */
    fun pushDatabaseFile()

    /**
     * @return The last modified date for the given output file as timestamp
     */
    fun outputFileLastModified(runOutputsDir: String, outputFile: OutputFile): Long

    /**
     * Copy cached output files from one workflow run directory to another
     */
    fun copyCachedFiles(fromDir: String, toDir: String, files: Set<String>)

    /**
     * Execute task for the environment. Will consist of running a docker container to complete the task, and another
     * for downloading the given input files from remote sources, and possibly more for environment specific requirements.
     *
     * @param outputFilesIn: Output files coming from a task's current "Input Element." These will already exist in the
     * current environment's storage.
     * @param outputFilesOut: Output files coming from a task's current "Output Element." These need to end up in the
     * current environment's storage.
     * @param cachedInputFiles: Input files that exist in the current environment's storage.
     * @param downloadInputFiles: Input files that need to be downloaded from original sources.
     */
    fun executeTask(workflowRunDir: String, taskRunId: Int, taskConfig: TaskConfig, dockerImage: String,
                    dockerDataDir: String, command: String?, outputFilesIn: Set<OutputFile>, outputFilesOut: Set<OutputFile>,
                    cachedInputFiles: Set<CachedInputFile>, downloadInputFiles: Set<InputFile>)

    /**
     * Download input files from remote sources and load them into /run/$run-timestamp/inputs directory.
     *
     * Should involve running a docker container defined in [InputFile.downloadFileImage] with the command
     * in [InputFile.downloadFileCommand].
     *
     * This functionality also needs to exist in [executeTask]. This function is separate because the implementation
     * may differ when we need to run it alone.
     */
    fun downloadRemoteInputFiles(inputFiles: Set<InputFile>, dockerDataDir: String, workflowInputsDir: String)
}

/**
 * A representation for a file that exists in an executor environment workflow run directory
 *
 * @param path: The relative file path
 * @param workflowInputsDir: The executor specific /run/$run-timestamp/inputs directory where the file lives
 * @param lastModified: last modified date of file
 */
data class CachedInputFile(val path: String, val workflowInputsDir: String, val lastModified: Long)

/**
 * Interface that deals with environment specific functionality when directing a workflow remotely.
 * This will involve shipping the current executable off somewhere else where it will run with a LocallyDirectedExecutor
 */
interface RemoteDirectedExecutor {
    fun execute(executablePath: Path, configPath: Path)
}
