package krews

import java.nio.file.Paths

data class WFile(val path: String) {
    var resolvedPaths: List<String>? = null
        internal set

    fun parentDir(): String {
        val pathParts = path.split("/")
        return pathParts.joinToString("/", limit = pathParts.size - 1)
    }

    fun filename(): String {
        return path.split("/").last()
    }
}

// TODO: Add remote files from FTP / GCS

/*
 * TODO: Add ability to use wildcards / globs
 * We'll need a way to give the user real filenames resolved from the wildcards after processing.
 * We can do this by Adding an internally set field "resolvedPaths"
 */

// TODO: Add ability to read files locally