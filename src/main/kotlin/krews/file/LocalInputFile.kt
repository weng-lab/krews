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
data class LocalInputFile(val localPath: String, override val path: String = defaultPath(localPath)) : InputFile() {
    override fun downloadFileImage() = throw exception
    override fun downloadFileCommand(containerBaseDir: String) = throw exception
}

private fun defaultPath(localPath: String) = Paths.get(localPath).fileName.toString()

private val exception = IllegalStateException("LocalInputFile may not be used with remote executor.")