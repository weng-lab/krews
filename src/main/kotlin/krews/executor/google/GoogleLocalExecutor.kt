package krews.executor.google

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.genomics.v2alpha1.model.*
import krews.core.CapacityType
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.config.googleMachineType
import krews.core.TaskRunContext
import krews.executor.*
import krews.file.GSInputFile
import krews.file.InputFile
import krews.file.OutputFile
import mu.KotlinLogging
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"

const val DEFAULT_INPUT_DOWNLOAD_DIR = "/data"

class GoogleLocalExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Local Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir
    private val runningOperations: MutableSet<String> = ConcurrentHashMap.newKeySet<String>()

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

    override fun uploadFile(fromPath: String, toPath: String, backup: Boolean) {
        val localFilePath = Paths.get(workflowConfig.localFilesBaseDir, fromPath)
        val storageObject = gcsObjectPath(gcsBase, toPath)

        log.info { "Pushing file $localFilePath to object $storageObject in bucket $bucket" }
        uploadObject(googleStorageClient, bucket, storageObject, localFilePath)

        if (backup) {
            val backupObject = "$storageObject.backup"
            log.info { "Backing up file $storageObject to $backupObject" }
            copyObject(googleStorageClient, bucket, storageObject, bucket, backupObject)
        }
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
        virtualMachine.machineType = googleMachineType(taskConfig.google, taskRunContext.cpus, taskRunContext.memory)

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
            val outputFileObject = gcsPath(bucket, gcsBase, OUTPUTS_DIR, it.path)
            createDownloadAction(outputFileObject, taskRunContext.dockerDataDir, it.path)
        }
        actions.addAll(downloadOutputFileActions)

        // Create the action that runs the task
        actions.add(createExecuteTaskAction(taskRunContext.dockerImage, taskRunContext.dockerDataDir, taskRunContext.command, taskRunContext.env))

        // Create the actions to upload each task output OutputFile
        val uploadActions = outputFilesOut.map {
            val outputFileObject = gcsPath(bucket, gcsBase, OUTPUTS_DIR, it.path)
            createUploadAction(outputFileObject, taskRunContext.dockerDataDir, it.path)
        }
        actions.addAll(uploadActions)

        // Create the action to upload all data dir files to diagnostic-output directory if execution fails
        val diagnosticsGSPath = gcsPath(bucket, gcsBase, workflowRunDir, DIAGNOSTICS_DIR, taskRunId.toString())
        val diagnosticsAction = createDiagnosticUploadAction(diagnosticsGSPath, taskRunContext.dockerDataDir)
        actions.add(diagnosticsAction)

        // Create action to copy logs to GCS after everything else is complete
        actions.add(createLogsAction(logPath))

        submitJobAndWait(run, googleConfig.jobCompletionPollInterval, "task run $taskRunId", runningOperations)
    }

    override fun shutdownRunningTasks() {
        for(op in runningOperations) {
            log.info { "Canceling operation $op..." }
            googleGenomicsClient.projects().operations().cancel(op, null)
        }
    }

    override fun downloadInputFile(inputFile: InputFile) {
        val toObjectPath = gcsObjectPath(gcsBase, INPUTS_DIR, inputFile.path)

        // If the other file is another google storage file, we can copy directly from bucket to bucket without downloading first
        if (inputFile is GSInputFile) {
            copyObject(googleStorageClient, inputFile.bucket, inputFile.objectPath, bucket, toObjectPath)
            return
        }

        val downloadBasePath = Paths.get(DEFAULT_INPUT_DOWNLOAD_DIR)
        val downloadedPath = downloadBasePath.resolve(inputFile.path)
        inputFile.downloadLocal(downloadBasePath)
        uploadObject(googleStorageClient, bucket, toObjectPath, downloadedPath)
        Files.delete(downloadedPath)
    }

    override fun listFiles(baseDir: String): Set<String> {
        val bucketContents = googleStorageClient.objects().list(bucket).setPrefix(gcsObjectPath(gcsBase, baseDir)).execute()
        return bucketContents.items?.map { it.name }?.toSet() ?: setOf()
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
    val tmpDir = "$dockerDataDir/tmp"
    if (command != null) action.commands = listOf("/bin/sh", "-c", "[ ! -d $tmpDir ] && mkdir $tmpDir; TMPDIR=$tmpDir;\n $command")
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
internal fun submitJobAndWait(run: RunPipelineRequest, jobCompletionPollInterval: Int, context: String, runningOperations: MutableSet<String>) {
    log.info { "Submitting pipeline job for task run: $run" }
    googleGenomicsClient.projects().operations()
    val initialOp: Operation = googleGenomicsClient.pipelines().run(run).execute()
    val opName = initialOp.name
    runningOperations.add(opName)

    log.info { "Pipeline job submitted. Operation returned: \"$opName\". " +
            "Will check for completion every $jobCompletionPollInterval seconds" }
    var retry = false
    var retryCount = 0
    do {
        var done = false
        if (retry) {
            retry = false
            retryCount++
        } else {
            Thread.sleep(jobCompletionPollInterval * 1000L)
            retryCount = 0
        }
        try {
            val op: Operation = googleGenomicsClient.projects().operations().get(opName).execute()
            if (op.done) {
                runningOperations.remove(opName)
                if (op.error != null) {
                    throw Exception("Error occurred during $context ($opName) execution. Operation Response: ${op.toPrettyString()}")
                }
                log.info { "Pipeline job for $context ($opName) completed successfully. Results: ${op.toPrettyString()}" }
            } else {
                log.info { "Pipeline job for task run $context ($opName) still running..." }
            }
            done = op.done
        }catch(e: IOException) {
            // 503 is service unavailable. When we catch this error, retry up to 2 times before failing.
            if (e is GoogleJsonResponseException && e.statusCode == 503) {
                if (retryCount < 2) {
                    log.info { "Pipeline request for $context ($opName) failed with 503. Retrying..." }
                    retry = true
                } else {
                    throw Exception("Pipeline request for $context ($opName) failed with 503 too many times.", e)
                }
            } else {
                throw Exception("Pipeline request for $context ($opName) failed.", e)
            }
        }
    } while (!done)
}
