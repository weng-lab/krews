package krews.file

import java.nio.file.Files
import java.nio.file.Paths

/**
 * An input file that refers to a file stored on a locally accessible file system. This is a special type of InputFile
 * that does not download files using containers and may only be used with the local executor.
 *
 * @param localPath: Path to locally accessible file.
 * @param path: Unique relative path used by Krews for storage and in task containers. Set to objectPath by default.
 */
class LocalInputFile(val localPath: String, path: String) : InputFile(path) {
    override fun fetchLastModified() = Files.getLastModifiedTime(Paths.get(localPath)).toMillis()
    override fun downloadFileImage() = throw exception
    override fun downloadFileCommand(containerBaseDir: String) = throw exception

    override fun toString() = "LocalInputFile(localPath='$localPath')"
}

private val exception = IllegalStateException("LocalInputFile may only be used with local executor")