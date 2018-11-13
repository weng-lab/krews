package krews.file

abstract class BaseFile(val path: String) {
    fun parentDir(): String {
        val pathParts = path.split("/")
        return pathParts.joinToString("/", limit = pathParts.size - 1)
    }

    fun filename(): String {
        return path.split("/").last()
    }

    fun filenameNoExt(): String {
        val filename = filename()
        return """(.*?)\..*""".toRegex().find(filename)?.groups?.get(1)?.value ?: filename
    }
}