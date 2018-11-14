package krews.file

import org.joda.time.DateTime

/**
 * Krews' representation of a File that results from running a task.
 *
 * These files end up in a /run/$run-timestamp/outputs directory. The storage method used depends on the executor.
 * For example, the local executor stores run files a local file system directory and
 * the google executor stores run files in google cloud storage.
 *
 * When used in task inputs, they are copied from storage into the docker container before running.
 * When used in task output, they are copied out of the docker container into storage.
 *
 * OutputFile objects in task inputs and outputs can be detected if the input / output types are WFiles themselves,
 * any type of collection or map containing WFiles, a data class with a WFile field, or any combination of the above.
 * For example, an input type of `data class MyData(val myMap: Map<String, WFile>)` would copy all WFiles in
 * MyData.myMap values.
 */
class OutputFile(path: String) : BaseFile(path) {
    var lastModified: Long = -1
        internal set

    override fun toString(): String {
        return "OutputFile(path=$path, lastModified=$lastModified)"
    }
}

/* TODO: finish implementing
class WildcardOutputFiles(val path: String) {
    lateinit var resolved: List<OutputFile> internal set
}
*/