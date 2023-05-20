package krews.executor.google

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.lifesciences.v2beta.model.*
import kotlinx.coroutines.delay
import krews.config.*
import krews.core.*
import krews.executor.*
import krews.file.*
import mu.KotlinLogging
import retry
import java.nio.file.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean


private val log = KotlinLogging.logger {}

const val CLOUD_SDK_IMAGE = "google/cloud-sdk:alpine"

class GoogleLocalExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    private val googleConfig = checkNotNull(workflowConfig.google)
        { "google workflow config must be present to use Google Local Executor" }
    private val bucket = googleConfig.bucket
    private val gcsBase = workflowConfig.workingDir
    private val runningOperations: MutableSet<String> = ConcurrentHashMap.newKeySet()

    private val allShutdown = AtomicBoolean(false)

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

    override fun uploadFile(fromPath: Path, toPath: String) {
        val storageObject = gcsObjectPath(gcsBase, toPath)

        log.info { "Pushing file $fromPath to object $storageObject in bucket $bucket" }
        uploadObject(googleStorageClient, bucket, storageObject, fromPath)
    }

    override fun fileExists(path: String): Boolean {
        try {
            googleStorageClient.objects().get(bucket, gcsObjectPath(gcsBase, path)).execute()
        } catch (e: GoogleJsonResponseException) {
            if (e.statusCode == 404) return false
        }
        return true
    }

    override suspend fun executeTask(
        workflowRunDir: String,
        taskRunId: Int,
        taskConfig: TaskConfig,
        taskRunContexts: List<TaskRunContext<*, *>>
    ) {
        if (allShutdown.get()) {
            throw Exception("shutdownRunningTasks has already been called")
        }

        val run = createRunPipelineRequest(googleConfig)
        val actions = run.pipeline.actions

        val virtualMachine = VirtualMachine()
        run.pipeline.resources.virtualMachine = virtualMachine
        val maxCpus = taskRunContexts.map { it.cpus }.maxBy { it ?: -1 } ?: taskConfig.google?.cpus
        val maxMemory = taskRunContexts.map { it.memory }.maxBy { it?.bytes ?: -1 } ?: taskConfig.google?.mem
        virtualMachine.machineType = googleMachineType(taskConfig.google, maxCpus, maxMemory)
        if (taskConfig.google?.bootDiskSizeGb != null)
            virtualMachine.setBootDiskSizeGb(taskConfig.google.bootDiskSizeGb)

        if (taskConfig.google?.gpus != null) {
            val acceleratorConfig = Accelerator()
            acceleratorConfig.type = taskConfig.google.gpus.gpuType
            acceleratorConfig.count = taskConfig.google.gpus.gpuCount
            virtualMachine.accelerators = listOf(acceleratorConfig)
            if (taskConfig.google.gpus.bootImage != null)
                virtualMachine.bootImage = taskConfig.google.gpus.bootImage
        }

        val serviceAccount = ServiceAccount()
        virtualMachine.serviceAccount = serviceAccount
        serviceAccount.scopes = listOf(STORAGE_READ_WRITE_SCOPE)

        val disk = Disk()
        virtualMachine.disks = listOf(disk)
        disk.name = DISK_NAME
        val diskSize = taskRunContexts.map { it.diskSize }.maxBy { it?.bytes ?: -1 } ?: taskConfig.google?.diskSize
        if (diskSize != null) {
            disk.sizeGb = diskSize.toType(CapacityType.GB).toInt()
        }

        // Create action to periodically copy logs to GCS
        val logPath = gcsPath(bucket, gcsBase, workflowRunDir, LOGS_DIR, taskRunId.toString(), LOG_FILE_NAME)
        actions.add(createPeriodicLogsAction(logPath, googleConfig.logUploadInterval))

        // Create a unique set of inputFiles to dockerDataDir combinations
        val inputFiles = taskRunContexts
            .flatMap { c -> c.inputFiles.map { f -> Pair(f, c.inputsDir) } }
            .toSet()

        // Create actions to download InputFiles from remote sources
        val downloadRemoteInputFileActions =
            inputFiles.map { createDownloadRemoteFileAction(it.first, it.second) }
        actions.addAll(downloadRemoteInputFileActions)

        // Create a unique set of outputFilesIn to dockerDataDir combinations
        val outputFilesIn = taskRunContexts
            .flatMap { c -> c.outputFilesIn.map { f -> Pair(f, c.inputsDir) } }
            .toSet()

        // Create actions to download each task input OutputFile from the current GCS run directory
        val downloadOutputFileActions = outputFilesIn.map {
            val outputFileObject = gcsPath(bucket, gcsBase, OUTPUTS_DIR, it.first.path)
            createDownloadAction(outputFileObject, it.second, it.first.path)
        }
        actions.addAll(downloadOutputFileActions)

        for (taskRunContext in taskRunContexts) {
            // Create the action that runs the task
            actions.add(
                createExecuteTaskAction(
                    taskRunContext.dockerImage,
                    taskRunContext.inputsDir,
                    taskRunContext.outputsDir,
                    taskRunContext.command,
                    taskRunContext.env,
                    taskConfig.google?.gpus != null
                )
            )

            // Create the actions to upload each task output OutputFile
            val uploadActions = taskRunContext.outputFilesOut.map {
                val outputFileObject = gcsPath(bucket, gcsBase, OUTPUTS_DIR, it.path)
                createUploadAction(outputFileObject, taskRunContext.outputsDir, it.path, it.optional)
            }
            actions.addAll(uploadActions)

            // Create the actions to upload each task output OutputDirectory
            val uploadDirectoryActions = taskRunContext.outputDirectoriesOut.map {
                val outputFileObject = gcsPath(bucket, gcsBase, OUTPUTS_DIR, it.path)
                createUploadDirectoryAction(outputFileObject, taskRunContext.outputsDir, it.path)
            }
            actions.addAll(uploadDirectoryActions)
        }

        // Create the action to upload all data dir files to diagnostic-output directory if execution fails
        for (dockerDataDir in taskRunContexts.map { it.outputsDir }.toSet()) {
            val diagnosticsGSPath = gcsPath(bucket, gcsBase, workflowRunDir, "$DIAGNOSTICS_DIR/outputs",
                taskRunId.toString(), dockerDataDir)
            val diagnosticsAction = createDiagnosticUploadAction(diagnosticsGSPath, dockerDataDir)
            actions.add(diagnosticsAction)
        }
        for (dockerDataDir in taskRunContexts.map { it.inputsDir }.toSet()) {
            val diagnosticsGSPath = gcsPath(bucket, gcsBase, workflowRunDir, "$DIAGNOSTICS_DIR/inputs",
                taskRunId.toString(), dockerDataDir)
            val diagnosticsAction = createDiagnosticUploadAction(diagnosticsGSPath, dockerDataDir)
            actions.add(diagnosticsAction)
        }

        // Create action to copy logs to GCS after everything else is complete
        actions.add(createLogsAction(logPath))

        val context = "task run $taskRunId"
        log.info { "Submitting pipeline job for task run: $run" }
        val initialOp: Operation = retry("Pipeline job submit",
            retryCondition = { e -> e is GoogleJsonResponseException && e.statusCode == 503 }) {
            val parent = "projects/${googleConfig.projectId}/locations/${googleConfig.lifeSciencesLocation}"
            googleLifeSciencesClient.projects().locations().pipelines().run(parent, run).execute()
        }
        val opName = initialOp.name
        runningOperations.add(opName)

        log.info {
            "Pipeline job submitted. Operation returned: \"$opName\". " +
                    "Will check for completion every ${googleConfig.jobCompletionPollInterval} seconds"
        }
        do {
            var done = false
            delay(googleConfig.jobCompletionPollInterval * 1000L)
            try {
                val op: Operation = googleLifeSciencesClient.projects().locations().operations().get(opName).execute()
                done = op.done == true
                if (done) {
                    runningOperations.remove(opName)
                    if (op.error != null) {
                        throw Exception("Error occurred during $context ($opName) execution. Operation Response: ${op.toPrettyString()}")
                    }
                    log.info { "Pipeline job for $context ($opName) completed successfully. Results: ${op.toPrettyString()}" }
                } else {
                    log.debug { "Pipeline job for task run $context ($opName) still running..." }
                }
            } catch(e: GoogleJsonResponseException) {
                if (e.statusCode != 503) {
                    throw e
                }
            }
        } while (!done)

        for (taskRunContext in taskRunContexts) {
            taskRunContext.outputDirectoriesOut.forEach { outDir ->
                val path = gcsObjectPath(gcsBase, OUTPUTS_DIR, outDir.path)
                outDir.filesFuture.complete(listFiles(googleStorageClient, bucket, path).map {
                    OutputFile(gcsObjectPath(gcsBase, OUTPUTS_DIR, outDir.path, it))
                })
            }
        }
    }

    override fun shutdownRunningTasks() {
        allShutdown.set(true)
        for (op in runningOperations) {
            log.info { "Canceling operation $op..." }
            googleLifeSciencesClient.projects().locations().operations().cancel(op, null)
        }
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

val NVIDIA_DOCKER_COMMANDS =
    """
    sudo cos-extensions install gpu;
    sudo mount --bind /var/lib/nvidia /var/lib/nvidia;
    sudo mount -o remount,exec /var/lib/nvidia
    """

/**
 * Create a pipeline action that will execute the task
 */
internal fun createExecuteTaskAction(
    dockerImage: String,
    inputsDir: String,
    outputsDir: String,
    command: String?,
    env: Map<String, String>?,
    gpus: Boolean
): Action {
    val action = Action()
    action.imageUri = dockerImage
    action.mounts = listOf(createMount(outputsDir), createMount(inputsDir))
    val tmpDir = "$outputsDir/tmp"
    val actionEnv = mutableMapOf("TMPDIR" to tmpDir)
    if (env != null) actionEnv.putAll(env)
    action.environment = actionEnv
    if (command != null) {
        if (gpus) action.commands = listOf("/bin/sh", "-c", "$NVIDIA_DOCKER_COMMANDS\n [ ! -d $tmpDir ] && mkdir $tmpDir;\n $command")
        else action.commands = listOf("/bin/sh", "-c", "[ ! -d $tmpDir ] && mkdir $tmpDir;\n $command")
    }
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
