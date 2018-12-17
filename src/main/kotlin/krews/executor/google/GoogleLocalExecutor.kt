package krews.executor.google

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.genomics.v2alpha1.model.*
import krews.config.CapacityType
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.core.TaskRunContext
import krews.executor.*
import krews.file.GSInputFile
import krews.file.InputFile
import krews.file.OutputFile
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Paths


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"

// Default VM machine type if not define in task configuration
const val DEFAULT_MACHINE_TYPE = "n1-standard-1"

const val DEFAULT_INPUT_DOWNLOAD_DIR = "/data"

class GoogleLocalExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Local Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir

    override fun downloadFile(path: String) {
        val localFilePath = Paths.get(workflowConfig.localFilesBaseDir, path)
        log.info { "Deleting local copy of $path if it exists" }
        Files.deleteIfExists(localFilePath)
        Files.createDirectories(localFilePath.parent)

        log.info { "Attempting to download $path from bucket $bucket..." }
        val storageObject = gcsObjectPath(gcsBase, path)
        val fileExists = downloadObject(googleStorageClient, bucket, storageObject, localFilePath)
        if (fileExists) {
            log.info { "$storageObject not found in bucket $bucket. A new database file will be used." }
        } else {
            log.info { "$storageObject successfully downloaded to $localFilePath" }
        }
    }

    override fun uploadFile(path: String) {
        val localFilePath = Paths.get(workflowConfig.localFilesBaseDir, path)
        val storageObject = gcsObjectPath(gcsBase, path)
        log.info { "Pushing file $localFilePath to object $storageObject in bucket $bucket" }
        uploadObject(googleStorageClient, bucket, storageObject, localFilePath)
    }

    override fun fileExists(path: String): Boolean {
        try {
            googleStorageClient.objects().get(bucket, gcsObjectPath(gcsBase, path)).execute()
        } catch (e: GoogleJsonResponseException) {
            if (e.statusCode == 404) return false
        }
        return true
    }

    override fun fileLastModified(path: String): Long {
        return googleStorageClient.objects().get(bucket, gcsObjectPath(gcsBase, path)).execute().updated.value
    }

    override fun executeTask(workflowRunDir: String,
                             taskRunId: Int,
                             taskConfig: TaskConfig,
                             taskRunContext: TaskRunContext<*, *>,
                             outputFilesIn: Set<OutputFile>,
                             outputFilesOut: Set<OutputFile>,
                             cachedInputFiles: Set<InputFile>,
                             downloadInputFiles: Set<InputFile>) {
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
        val downloadRemoteInputFileActions = downloadInputFiles.map { createDownloadRemoteFileAction(it, taskRunContext.dockerDataDir) }
        actions.addAll(downloadRemoteInputFileActions)

        // Create actions to download InputFiles from our GCS run directories
        val downloadLocalInputFileActions = cachedInputFiles.map {
            val inputFileObject = gcsPath(bucket, gcsBase, INPUTS_DIR, it.path)
            createDownloadAction(inputFileObject, taskRunContext.dockerDataDir, it.path)
        }
        actions.addAll(downloadLocalInputFileActions)

        // Create actions to download each task input OutputFile from the current GCS run directory
        val downloadOutputFileActions = outputFilesIn.map {
            val outputFileObject = gcsPath(bucket, gcsBase, workflowRunDir, OUTPUTS_DIR, it.path)
            createDownloadAction(outputFileObject, taskRunContext.dockerDataDir, it.path)
        }
        actions.addAll(downloadOutputFileActions)

        // Create the action that runs the task
        actions.add(createExecuteTaskAction(taskRunContext.dockerImage, taskRunContext.dockerDataDir, taskRunContext.command, taskRunContext.env))

        // Create the actions to upload each task output OutputFile
        val uploadActions = outputFilesOut.map {
            val outputFileObject = gcsPath(bucket, gcsBase, workflowRunDir, OUTPUTS_DIR, it.path)
            createUploadAction(outputFileObject, taskRunContext.dockerDataDir, it.path)
        }
        actions.addAll(uploadActions)

        // Create the action to upload all data dir files to diagnostic-output directory if execution fails
        val diagnosticsGSPath = gcsPath(bucket, gcsBase, workflowRunDir, DIAGNOSTICS_DIR, taskRunId.toString())
        val diagnosticsAction = createDiagnosticUploadAction(diagnosticsGSPath, taskRunContext.dockerDataDir)
        actions.add(diagnosticsAction)

        // Create action to copy logs to GCS after everything else is complete
        actions.add(createLogsAction(logPath))

        submitJobAndWait(run, googleConfig.jobCompletionPollInterval, "task run $taskRunId")
    }

    override fun downloadInputFile(inputFile: InputFile) {
        // If the other file is another google storage file, we can copy directly from bucket to bucket without downloading first
        if (inputFile is GSInputFile) {
            copyObject(googleStorageClient, inputFile.bucket, inputFile.objectPath, bucket, gcsObjectPath(gcsBase, inputFile.path))
            return
        }

        val downloadBasePath = Paths.get(DEFAULT_INPUT_DOWNLOAD_DIR)
        val downloadedPath = downloadBasePath.resolve(inputFile.path)
        inputFile.downloadLocal(downloadBasePath)
        uploadObject(googleStorageClient, bucket, gcsObjectPath(gcsBase, inputFile.path), downloadedPath)
        Files.delete(downloadedPath)
    }

    override fun listFiles(baseDir: String): Set<String> {
        val bucketContents = googleStorageClient.objects().list(bucket).setPrefix(gcsObjectPath(gcsBase, baseDir)).execute()
        return bucketContents.items.map { it.name }.toSet()
    }

    override fun deleteFile(file: String) {
        googleStorageClient.objects().delete(bucket, gcsObjectPath(gcsBase, file)).execute()
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