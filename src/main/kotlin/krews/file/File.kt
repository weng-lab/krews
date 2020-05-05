package krews.file

interface File {

    val path: String

    fun parentDir(): String {
        val pathParts = path.split("/")
        return pathParts.joinToString("/", limit = pathParts.size - 1)
    }

    fun filename(): String {
        return path.split("/").last()
    }

    /**
     * Returns the name of the file only. Given a path like
     * "path/to/file.txt.gz", this will return "file".
     */
    fun filenameNoExt(): String {
        val filename = filename()
        return """(.+\/)*(.*?)\..*""".toRegex().find(filename)?.groups?.get(2)?.value ?: filename
    }
}