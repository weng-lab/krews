package krews.executor

import krews.WFile
import krews.config.TaskConfig
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

/**
 * Interface that deals with environment specific functionality, ie. moving files around and running containers.
 */
interface EnvironmentExecutor {
    /**
     * Execute task for the environment. Will usually consist of running a docker container.
     */
    fun executeTask(workflowRunDir: String, taskConfig: TaskConfig, image: String, script: String?, inputItem: Any, outputItem: Any?)

    /**
     * Copy cached output files from one workflow run directory to another
     */
    fun copyCachedOutputs(fromWorkflowDir: String, toWorkflowDir: String, outputFiles: Set<WFile>)

    /**
     * Download the database file and return path if remote, otherwise just return path
     */
    fun prepareDatabaseFile(): String
}

/**
 * Finds all Krews WFile objects in the given object by recursively looking through the given object graph.
 */
@Suppress("UNCHECKED_CAST")
fun getFilesForObject(obj: Any?): Set<WFile> {
    return when (obj) {
        null, is String, is Number, is Boolean, is Char -> setOf()
        is WFile -> setOf(obj)
        is Array<*> -> obj.flatMap { getFilesForObject(it) }.toSet()
        is Collection<*> -> obj.flatMap { getFilesForObject(it) }.toSet()
        is Map<*,*> -> obj.values.flatMap { getFilesForObject(it) }.toSet()
        else -> obj::class.memberProperties.flatMap { getFilesForObject((it as KProperty1<Any, *>).get(obj)) }.toSet()
    }
}

