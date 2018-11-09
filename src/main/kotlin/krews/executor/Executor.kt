package krews.executor

import krews.config.TaskConfig
import krews.db.InputFileRecord
import krews.file.InputFile
import krews.file.OutputFile
import java.nio.file.Path
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

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
     * Copy cached output files from one workflow run directory to another
     */
    fun copyCachedFiles(fromDir: String, toDir: String, files: Set<String>)

    /**
     * Execute task for the environment. Will consist of running a docker container to complete the task, and another
     * for downloading the given input files from remote sources, and possibly more for environment specific requirements.
     *
     * @param outputFilesIn: Output files coming from a task's current "Input Item." These will already exist in the
     * current environment's storage.
     * @param outputFilesOut: Output files coming from a task's current "Output Item." These need to end up in the
     * current environment's storage.
     * @param localInputFiles: Input files that exist in the current environment's storage.
     * @param remoteInputFiles: Input files that need to be downloaded from remote sources.
     */
    fun executeTask(workflowRunDir: String, taskRunId: Int, taskConfig: TaskConfig, dockerImage: String,
                    dockerDataDir: String, command: String?, outputFilesIn: Set<OutputFile>, outputFilesOut: Set<OutputFile>,
                    localInputFiles: Set<LocalInputFile>, remoteInputFiles: Set<InputFile>)

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
data class LocalInputFile(val path: String, val workflowInputsDir: String, val lastModified: Long)

/**
 * Interface that deals with environment specific functionality when directing a workflow remotely.
 * This will involve shipping the current executable off somewhere else where it will run with a LocallyDirectedExecutor
 */
interface RemoteDirectedExecutor {
    fun execute(executablePath: Path, configPath: Path)
}

/**
 * Finds all Krews OutputFile objects in the given object by recursively looking through the given object graph.
 */
@Suppress("UNCHECKED_CAST")
fun getOutputFilesForObject(obj: Any?): Set<OutputFile> {
    return when (obj) {
        null, is String, is Number, is Boolean, is Char -> setOf()
        is OutputFile -> setOf(obj)
        is Array<*> -> obj.flatMap { getOutputFilesForObject(it) }.toSet()
        is Collection<*> -> obj.flatMap { getOutputFilesForObject(it) }.toSet()
        is Map<*,*> -> obj.values.flatMap { getOutputFilesForObject(it) }.toSet()
        else -> obj::class.memberProperties.flatMap { getOutputFilesForObject((it as KProperty1<Any, *>).get(obj)) }.toSet()
    }
}

/**
 * Finds all Krews InputFile objects in the given object by recursively looking through the given object graph.
 */
@Suppress("UNCHECKED_CAST")
fun getInputFilesForObject(obj: Any?): Set<InputFile> {
    return when (obj) {
        null, is String, is Number, is Boolean, is Char -> setOf()
        is InputFile -> setOf(obj)
        is Array<*> -> obj.flatMap { getInputFilesForObject(it) }.toSet()
        is Collection<*> -> obj.flatMap { getInputFilesForObject(it) }.toSet()
        is Map<*,*> -> obj.values.flatMap { getInputFilesForObject(it) }.toSet()
        else -> obj::class.memberProperties.flatMap { getInputFilesForObject((it as KProperty1<Any, *>).get(obj)) }.toSet()
    }
}

