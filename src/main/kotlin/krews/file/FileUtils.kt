package krews.file

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.*
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.streams.toList

/**
 * Finds all Krews OutputFile objects in the given object by recursively looking through the given object graph.
 */
@Suppress("UNCHECKED_CAST")
internal fun getOutputFilesForObject(obj: Any?): Set<OutputFile> {
    return when (obj) {
        null, is String, is Number, is Boolean, is Char -> setOf()
        is OutputFile -> setOf(obj)
        is Array<*> -> obj.flatMap { getOutputFilesForObject(it) }.toSet()
        is Collection<*> -> obj.flatMap { getOutputFilesForObject(it) }.toSet()
        is Map<*,*> -> obj.values.flatMap { getOutputFilesForObject(it) }.toSet()
        else ->
            try {
                obj::class.memberProperties.flatMap { getOutputFilesForObject((it as KProperty1<Any, *>).get(obj)) }
                    .toSet()
            } catch (e: Throwable) {
                Collections.emptySet<OutputFile>()
            }
    }
}

/**
 * Finds all Krews OutputDirectory objects in the given object by recursively looking through the given object graph.
 */
@Suppress("UNCHECKED_CAST")
internal fun getOutputDirectoriesForObject(obj: Any?): Set<OutputDirectory> {
    return when (obj) {
        null, is String, is Number, is Boolean, is Char -> setOf()
        is OutputDirectory -> setOf(obj)
        is Array<*> -> obj.flatMap { getOutputDirectoriesForObject(it) }.toSet()
        is Collection<*> -> obj.flatMap { getOutputDirectoriesForObject(it) }.toSet()
        is Map<*,*> -> obj.values.flatMap { getOutputDirectoriesForObject(it) }.toSet()
        else ->
            try {
                obj::class.memberProperties.flatMap { getOutputDirectoriesForObject((it as KProperty1<Any, *>).get(obj)) }
                    .toSet()
            } catch (e: Throwable) {
                Collections.emptySet<OutputDirectory>()
            }
    }
}

/**
 * Finds all Krews InputFile objects in the given object by recursively looking through the given object graph.
 */
@Suppress("UNCHECKED_CAST")
internal fun getInputFilesForObject(obj: Any?): Set<InputFile> {
    return when (obj) {
        null, is String, is Number, is Boolean, is Char -> setOf()
        is InputFile -> setOf(obj)
        is Array<*> -> obj.flatMap { getInputFilesForObject(it) }.toSet()
        is Collection<*> -> obj.flatMap { getInputFilesForObject(it) }.toSet()
        is Map<*,*> -> obj.values.flatMap { getInputFilesForObject(it) }.toSet()
        else ->
            try {
                obj::class.memberProperties.flatMap { getInputFilesForObject((it as KProperty1<Any, *>).get(obj)) }
                    .toSet()
            } catch (e: Throwable) {
                Collections.emptySet<InputFile>()
            }
    }
}

/**
 * Lists local file system files in the given directory
 */
fun listLocalFiles(dir: Path): Set<String> {
    if (!Files.exists(dir)) return setOf()
    return Files.walk(dir)
        .filter { Files.isRegularFile(it) }
        .map { it.toString() }
        .toList().toSet()
}