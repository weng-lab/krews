package krews.executor.google

import com.google.api.services.genomics.v2alpha1.model.*
import krews.config.CapacityType
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.executor.*
import krews.file.InputFile
import krews.file.OutputFile
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Paths


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"

// Default VM machine type if not define in task configuration
const val DEFAULT_MACHINE_TYPE = "n1-standard-1"

class GoogleLocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Local Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir
    private val dbFilePath = Paths.get(googleConfig.localStorageBaseDir, DB_FILENAME).toAbsolutePath()
    private val dbStorageObject = gcsObjectPath(gcsBase, STATE_DIR, DB_FILENAME)

    override fun prepareDatabaseFile(): String {
        log.info { "Deleting local copy of $dbFilePath if it exists" }
        Files.deleteIfExists(dbFilePath)
        Files.createDirectories(Paths.get(googleConfig.localStorageBaseDir))

        log.info { "Attempting to download $dbStorageObject from bucket $bucket..." }
        val fileExists = downloadObject(googleStorageClient, bucket, dbStorageObject, dbFilePath)
        if (fileExists) {
            log.info { "$dbStorageObject not found in bucket $bucket. A new database file will be used." }
        } else {
            log.info { "$dbStorageObject successfully downloaded to $dbFilePath" }
        }
        return dbFilePath.toString()
    }

    override fun pushDatabaseFile() {
        log.info { "Pushing database file $dbFilePath to object $dbStorageObject in bucket $bucket" }
        uploadObject(googleStorageClient, bucket, dbStorageObject, dbFilePath)
    }

    override fun outputFileLastModified(runOutputsDir: String, outputFile: OutputFile): Long {
        val objectPath = gcsObjectPath(gcsBase, runOutputsDir, outputFile.path)
        return googleStorageClient.objects().get(bucket, objectPath).execute().updated.value
    }

    override fun copyCachedFiles(fromDir: String, toDir: String, files: Set<String>) {
        for (file in files) {
            val fromObject = gcsObjectPath(gcsBase, fromDir, file)
            val toObject = gcsObjectPath(gcsBase, toDir, file)
            copyObject(googleStorageClient, bucket, fromObject, bucket, toObject)
        }
    }

    override fun executeTask(workflowRunDir: String, taskRunId: Int, taskConfig: TaskConfig, dockerImage: String,
                             dockerDataDir: String, command: String?, outputFilesIn: Set<OutputFile>, outputFilesOut: Set<OutputFile>,
                             cachedInputFiles: Set<CachedInputFile>, downloadInputFiles: Set<InputFile>) {
        val run = createRunPipelineRequest(googleConfig)
        val actions = run.pipeline.actions

        val virtualMachine = VirtualMachine()
        run.pipeline.resources.virtualMachine = virtualMachine
        virtualMachine.machineType = taskConfig.google?.machineType ?: DEFAULT_MACHINE_TYPE

        val serviceAccount = ServiceAccount()
        virtualMachine.serviceAccount = serviceAccount
        serviceAccount.scopes = listOf(STORAGE_READ_WRITE_SCOPE)

        val disk = Disk()
        virtualMachine.disks = listOf(disk)
        disk.name = DISK_NAME
        if (taskConfig.google?.diskSize != null) {
            disk.sizeGb = taskConfig.google.diskSize.toType(CapacityType.GB).toInt()
        }

        // Create action to periodically copy logs to GCS
        val logPath = gcsPath(bucket, gcsBase, workflowRunDir, LOGS_DIR, taskRunId.toString(), LOG_FILE_NAME)
        actions.add(createPeriodicLogsAction(logPath, googleConfig.logUploadInterval))

        // Create actions to download InputFiles from remote sources
        val downloadRemoteInputFileActions = downloadInputFiles.map { createDownloadRemoteFileAction(it, dockerDataDir) }
        actions.addAll(downloadRemoteInputFileActions)

        // Create actions to download InputFiles from our GCS run directories
        val downloadLocalInputFileActions = cachedInputFiles.map {
            val inputFileObject = gcsPath(bucket, gcsBase, it.workflowInputsDir, it.path)
            createDownloadAction(inputFileObject, dockerDataDir, it.path)
        }
        actions.addAll(downloadLocalInputFileActions)

        // Create actions to download each task input OutputFile from the current GCS run directory
        val downloadOutputFileActions = outputFilesIn.map {
            val outputFileObject = gcsPath(bucket, gcsBase, workflowRunDir, OUTPUTS_DIR, it.path)
            createDownloadAction(outputFileObject, dockerDataDir, it.path)
        }
        actions.addAll(downloadOutputFileActions)

        // Create the action that runs the task
        actions.add(createExecuteTaskAction(dockerImage, dockerDataDir, command, taskConfig.env))

        // Create the actions to upload each downloaded remote InputFile
        val uploadInputFileActions = downloadInputFiles.map {
            val inputFileObject = gcsPath(bucket, gcsBase, workflowRunDir, INPUTS_DIR, it.path)
            createUploadAction(inputFileObject, dockerDataDir, it.path)
        }
        actions.addAll(uploadInputFileActions)

        // Create the actions to upload each task output OutputFile
        val uploadActions = outputFilesOut.map {
            val outputFileObject = gcsPath(bucket, gcsBase, workflowRunDir, OUTPUTS_DIR, it.path)
            createUploadAction(outputFileObject, dockerDataDir, it.path)
        }
        actions.addAll(uploadActions)

        // Create action to copy logs to GCS after everything else is complete
        actions.add(createLogsAction(logPath))

        submitJobAndWait(run, googleConfig.jobCompletionPollInterval, "task run $taskRunId")
    }

    override fun downloadRemoteInputFiles(inputFiles: Set<InputFile>, dockerDataDir: String, workflowInputsDir: String) {
        val run = createRunPipelineRequest(googleConfig)
        val actions = inputFiles.map { createDownloadRemoteFileAction(it, dockerDataDir) }
        run.pipeline.actions.addAll(actions)

        // Create the actions to upload each downloaded remote InputFile
        val uploadInputFileActions = inputFiles.map {
            val inputFileObject = gcsPath(bucket, gcsBase, workflowInputsDir, it.path)
            createUploadAction(inputFileObject, dockerDataDir, it.path)
        }
        run.pipeline.actions.addAll(uploadInputFileActions)

        submitJobAndWait(run, googleConfig.jobCompletionPollInterval, "remote file download")
    }

}

/**
 * Create a pipeline action that will execute the task
 */
internal fun createExecuteTaskAction(dockerImage: String, dockerDataDir: String, command: String?, env: Map<String, String>?): Action {
    val action = Action()
    action.imageUri = dockerImage
    action.mounts = listOf(createMount(dockerDataDir))
    action.environment = env
    if (command != null) action.commands = listOf("/bin/sh", "-c", command)
    return action
}

/**
 * Create pipeline actions that will download input files from remote sources
 */
internal fun createDownloadRemoteFileAction(inputFile: InputFile, dataDir: String): Action {
    val action = Action()
    action.imageUri = inputFile.downloadFileImage()
    action.mounts = listOf(createMount(dataDir))
    action.commands = listOf("/bin/sh", "-c", inputFile.downloadFileCommand(dataDir))
    return action
}

/**
 * Submits a job to the pipelines api and polls periodically until the job is complete.
 * The current thread will be blocked until the job is complete.
 */
internal fun submitJobAndWait(run: RunPipelineRequest, jobCompletionPollInterval: Int, context: String) {
    log.info { "Submitting pipeline job for task run: $run" }
    googleGenomicsClient.projects().operations()
    val initialOp: Operation = googleGenomicsClient.pipelines().run(run).execute()

    log.info { "Pipeline job submitted. Operation returned: \"${initialOp.name}\". " +
            "Will check for completion every $jobCompletionPollInterval seconds" }
    do {
        Thread.sleep(jobCompletionPollInterval * 1000L)
        val op: Operation = googleGenomicsClient.projects().operations().get(initialOp.name).execute()
        if (op.done) {
            if (op.error != null) {
                throw Exception("Error occurred during $context (${op.name}) execution. Operation Response: ${op.toPrettyString()}")
            }
            log.info { "Pipeline job for $context (${op.name}) completed successfully. Results: ${op.toPrettyString()}" }
        } else {
            log.info { "Pipeline job for task run $context (${op.name}) still running..." }
        }
    } while (!op.done)
}