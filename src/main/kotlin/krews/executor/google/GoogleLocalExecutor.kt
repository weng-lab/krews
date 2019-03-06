package krews.executor.google

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.genomics.v2alpha1.model.*
import krews.core.CapacityType
import krews.config.TaskConfig
import krews.config.WorkflowConfig
import krews.config.googleMachineType
import krews.core.Task
import krews.core.TaskRunContext
import krews.core.TaskRunner
import krews.executor.*
import krews.executor.slurm.SlurmCheckEmptyResponseException
import krews.executor.slurm.SlurmJobState
import krews.executor.slurm.SlurmJobStateCategory
import krews.file.GSInputFile
import krews.file.InputFile
import krews.file.OutputFile
import mu.KotlinLogging
import retry
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.pow


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"

const val DEFAULT_INPUT_DOWNLOAD_DIR = "/data"

class GoogleLocalExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Local Executor" }
    private val bucket = googleConfig.storageBucket
    private val gcsBase = googleConfig.storageBaseDir
    private val runningOperations: MutableSet<String> = ConcurrentHashMap.newKeySet<String>()

    private var allShutdown = false

    override fun downloadFile(fromPath: String, toPath: Path) {
        log.info { "Attempting to download $fromPath from bucket $bucket to $toPath..." }
        Files.createDirectories(toPath.parent)
        val storageObject = gcsObjectPath(gcsBase, fromPath)
        val fileExists = downloadObject(googleStorageClient, bucket, storageObject, toPath)
        if (!fileExists) {
            log.info { "$storageObject not found in bucket $bucket." }
        } else {
            log.info { "$storageObject successfully downloaded to $toPath" }
        }
    }

    override fun uploadFile(fromPath: Path, toPath: String, backup: Boolean) {
        val storageObject = gcsObjectPath(gcsBase, toPath)

        log.info { "Pushing file $fromPath to object $storageObject in bucket $bucket" }
        uploadObject(googleStorageClient, bucket, storageObject, fromPath)

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

    private inner class GoogleJob(val opName: String, val context: String) : Future<Unit> {
        private var capturedThrowable: Throwable? = null
        private var lastOperation: Operation? = null
        private var cancelled = false

        private fun checkStatus(): Operation? {
            if (lastOperation?.done == true) {
                return lastOperation
            }
            var theop: Operation? = null
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
                theop = op
                lastOperation = op
            } catch(e: GoogleJsonResponseException) {
                if (e.statusCode === 503) {
                    return lastOperation
                }
                capturedThrowable = e
            } catch (e: Throwable) {
                capturedThrowable = e
            }
            return theop
        }

        override fun isDone(): Boolean {
            val op: Operation? = checkStatus()
            return capturedThrowable != null || (op != null && op.done)
        }

        override fun get() {
            // An arbitrarily high value that won't overflow the long
            return get(30, TimeUnit.DAYS)
        }

        override fun get(timeout: Long, unit: TimeUnit?) {
            val startTime: Long = System.currentTimeMillis()
            do {
                if (capturedThrowable != null) {
                    throw capturedThrowable!!
                }
                val waitTime = TimeUnit.MILLISECONDS.convert(timeout, (unit ?: TimeUnit.MILLISECONDS))
                if (startTime + waitTime > System.currentTimeMillis()) {
                    throw TimeoutException()
                }
            } while (!isDone)
        }


        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            if (!mayInterruptIfRunning && opName in runningOperations) {
                return false
            }
            if (!runningOperations.remove(opName)) {
                return false
            }
            googleGenomicsClient.projects().operations().cancel(opName, null)
            cancelled = true
            return true
        }

        override fun isCancelled(): Boolean {
            return allShutdown || cancelled
        }
    }

    override fun executeTask(
        workflowRunDir: String,
        taskRunId: Int,
        taskConfig: TaskConfig,
        taskRunContext: TaskRunContext<*, *>,
        outputFilesIn: Set<OutputFile>,
        outputFilesOut: Set<OutputFile>,
        cachedInputFiles: Set<InputFile>,
        downloadInputFiles: Set<InputFile>
    ): Future<Unit> {
        if (allShutdown) {
            throw Exception("shutdownRunningTasks has already been called")
        }
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
        val downloadRemoteInputFileActions =
            downloadInputFiles.map { createDownloadRemoteFileAction(it, taskRunContext.dockerDataDir) }
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
        actions.add(
            createExecuteTaskAction(
                taskRunContext.dockerImage,
                taskRunContext.dockerDataDir,
                taskRunContext.command,
                taskRunContext.env
            )
        )

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

        log.info { "Submitting pipeline job for task run: $run" }
        googleGenomicsClient.projects().operations()
        val initialOp: Operation = retry("Pipeline job submit",
            retryCondition = { e -> e is GoogleJsonResponseException && e.statusCode == 503 }) {
            googleGenomicsClient.pipelines().run(run).execute()
        }
        val opName = initialOp.name
        runningOperations.add(opName)

        log.info {
            "Pipeline job submitted. Operation returned: \"$opName\". " +
                    "Will check for completion every ${googleConfig.jobCompletionPollInterval} seconds"
        }

        return GoogleJob(opName, "task run $taskRunId")
    }

    override fun shutdownRunningTasks() {
        allShutdown = true
        for (op in runningOperations) {
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
        val bucketContents =
            googleStorageClient.objects().list(bucket).setPrefix(gcsObjectPath(gcsBase, baseDir)).execute()
        return bucketContents.items?.map { it.name }?.toSet() ?: setOf()
    }

    override fun deleteFile(file: String) {
        googleStorageClient.objects().delete(bucket, gcsObjectPath(gcsBase, file)).execute()
    }

}

/**
 * Create a pipeline action that will execute the task
 */
internal fun createExecuteTaskAction(
    dockerImage: String,
    dockerDataDir: String,
    command: String?,
    env: Map<String, String>?
): Action {
    val action = Action()
    action.imageUri = dockerImage
    action.mounts = listOf(createMount(dockerDataDir))
    val tmpDir = "$dockerDataDir/tmp"
    val actionEnv = mutableMapOf("TMPDIR" to tmpDir)
    if (env != null) actionEnv.putAll(env)
    action.environment = actionEnv
    if (command != null) action.commands = listOf("/bin/sh", "-c", "[ ! -d $tmpDir ] && mkdir $tmpDir;\n $command")
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
