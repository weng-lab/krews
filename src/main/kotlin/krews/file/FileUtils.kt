package krews.file

import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

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
        else -> obj::class.memberProperties.flatMap { getOutputFilesForObject((it as KProperty1<Any, *>).get(obj)) }.toSet()
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
        else -> obj::class.memberProperties.flatMap { getInputFilesForObject((it as KProperty1<Any, *>).get(obj)) }.toSet()
    }
}
