package krews.file

import com.fasterxml.jackson.annotation.JsonView
import krews.misc.ConfigView
import java.nio.file.Path

/**
 * Remote files downloaded and used for tasks.
 *
 * When used in task inputs, they are copied directly from the remote source into the docker container before running.
 * Upon completion of the task, they are copied into the /run/$run-timestamp/inputs directory
 *
 * @param path The relative path for this input file. It will be used as the storage path under the
 *      /run/$run-timestamp/inputs directory, as well as the local task docker container path.
 */
abstract class InputFile(path: String,
                         @field:JsonView(ConfigView::class) val cache: Boolean = false) : File(path) {

    val lastModified: Long by lazy { fetchLastModified() }

    /**
     * Retrieve the last modified timestamp
     */
    internal abstract fun fetchLastModified(): Long

    /**
     * The docker image used to download the input file
     */
    internal abstract fun downloadFileImage(): String

    /**
     * The docker command used to download the input file
     *
     * @param containerBaseDir: The container local directory that the input file will be downloaded to
     */
    internal abstract fun downloadFileCommand(containerBaseDir: String): String

    /**
     * Download locally on the master process.
     *
     * @param toBaseDir: The local directory that the input file will be downloaded to
     */
    internal abstract fun downloadLocal(toBaseDir: Path)
}