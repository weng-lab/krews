package krews.file

import org.joda.time.DateTime

/**
 * Remote files downloaded and used for tasks.
 *
 * When used in task inputs, they are copied directly from the remote source into the docker container before running.
 * Upon completion of the task, they are copied into the /run/$run-timestamp/inputs directory
 *
 * @param path The relative path for this input file. It will be used as the storage path under the
 *      /run/$run-timestamp/inputs directory, as well as the local task docker container path.
 */
abstract class InputFile(path: String) : BaseFile(path) {
    val lastModified: DateTime by lazy { fetchLastModified() }

    /**
     * Retrieve the last modified timestamp
     */
    abstract fun fetchLastModified(): DateTime

    /**
     * The docker image used to download the input file
     */
    abstract fun downloadFileImage(): String

    /**
     * The docker command used to download the input file
     *
     * @param containerBaseDir: The container local directory that the input file will be downloaded to
     */
    abstract fun downloadFileCommand(containerBaseDir: String): String
}