package krews.file

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * An input file that refers to a file stored on a locally accessible file system. This is a special type of InputFile
 * that does not download files using containers and may only be used with the local executor.
 *
 * @param localPath: Path to locally accessible file.
 * @param path: Unique relative path used by Krews for storage and in task containers. Set to objectPath by default.
 */
class LocalInputFile(val localPath: String, path: String = defaultPath(localPath)) : InputFile(path) {

    override fun fetchLastModified(): Long {
        val localPath = Paths.get(this.localPath)
        if (!Files.exists(localPath)) return -1
        return Files.getLastModifiedTime(localPath).toMillis()
    }
    override fun downloadFileImage() = throw exception
    override fun downloadFileCommand(containerBaseDir: String) = throw exception

    override fun toString() = "LocalInputFile(localPath='$localPath')"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LocalInputFile

        if (localPath != other.localPath) return false
        if (path != other.path) return false

        return true
    }

    override fun hashCode(): Int {
        var result = localPath.hashCode()
        result = 31 * result + path.hashCode()
        return result
    }


}

private fun defaultPath(localPath: String) = Paths.get(localPath).fileName.toString()

private val exception = IllegalStateException("LocalInputFile may not be used with remote executor.")