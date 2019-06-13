package krews.file

import mu.KotlinLogging
import retry

private val log = KotlinLogging.logger {}

/**
 * Remote files downloaded and used for tasks.
 *
 * When used in task inputs, they are copied directly from the remote source into the docker container before running.
 * Upon completion of the task, they are copied into the /run/$run-timestamp/inputs directory
 *
 * @param path The relative path for this input file. It will be used as the storage path under the
 *      /run/$run-timestamp/inputs directory, as well as the local task docker container path.
 */
abstract class InputFile : File {

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
}