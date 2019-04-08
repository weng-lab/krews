package krews.executor.slurm

import kotlinx.coroutines.delay
import krews.config.*
import krews.core.*
import krews.executor.LocallyDirectedExecutor
import krews.file.*
import krews.misc.CommandExecutor
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import java.nio.file.*
import java.util.*
import java.util.UUID.randomUUID
import retrySuspend
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.pow

private val log = KotlinLogging.logger {}

const val RUN_SCRIPT_NAME = "run_script.sh"

class SlurmExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    // Slurm "Job Name" that will serve as an identifier for all jobs created by this workflow.
    // We will use this to delete jobs without needing to track individual job ids.
    private val slurmWorkflowJobName = randomUUID().toString()

    private val commandExecutor = CommandExecutor(workflowConfig.slurm?.ssh)
    private val workflowBasePath = Paths.get(workflowConfig.workingDir).toAbsolutePath()!!
    private val outputsPath = workflowBasePath.resolve(OUTPUTS_DIR)

    private var allShutdown = AtomicBoolean(false)

    override fun downloadFile(fromPath: String, toPath: Path) {
        val fromFile = workflowBasePath.resolve(fromPath)
        log.info { "Attempting to copy $fromFile to $toPath..." }
        val fileExists = Files.exists(fromFile)
        if (fileExists) {
            Files.createDirectories(toPath.parent)
            FileUtils.copyFile(fromFile.toFile(), toPath.toFile())
            log.info { "$fromFile successfully copied to $toPath!" }
        } else {
            log.info { "$fromFile not found. It will not be copied." }
        }
    }

    override fun uploadFile(fromPath: Path, toPath: String, backup: Boolean) {
        val toFile = workflowBasePath.resolve(toPath)
        log.info { "Copying file $fromPath to $toFile" }
        Files.createDirectories(toFile.parent)
        FileUtils.copyFile(fromPath.toFile(), toFile.toFile())

        if (backup) {
            val backupFile = workflowBasePath.resolve("$toFile.backup")
            log.info { "Backing up file $toFile to $backupFile" }
            FileUtils.copyFile(toFile.toFile(), backupFile.toFile())
        }
    }

    override fun fileExists(path: String): Boolean = Files.exists(workflowBasePath.resolve(path))
    override fun fileLastModified(path: String): Long =
        Files.getLastModifiedTime(workflowBasePath.resolve(path)).toMillis()

    override fun listFiles(baseDir: String): Set<String> = listLocalFiles(workflowBasePath.resolve(baseDir))
    override fun deleteFile(file: String) = Files.delete(workflowBasePath.resolve(file))

    override suspend fun executeTask(
        workflowRunDir: String,
        taskRunId: Int,
        taskConfig: TaskConfig,
        taskRunContexts: List<TaskRunContext<*, *>>
    ) {
        if (allShutdown.get()) {
            throw Exception("shutdownRunningTasks has already been called")
        }
        val runBasePath = workflowBasePath.resolve(workflowRunDir)
        val logsPath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())
        logsPath.toFile().mkdirs()

        val sbatchScript = StringBuilder("#!/bin/bash\n#\n")

        val mem = taskConfig.slurm?.mem ?: taskRunContexts.map { it.memory }.maxBy { it?.bytes ?: -1 }
        val cpus = taskConfig.slurm?.cpus ?: taskRunContexts.map { it.cpus }.maxBy { it ?: -1 }
        val timePerTask = taskConfig.slurm?.time ?: taskRunContexts.map { it.cpus }.maxBy { it ?: -1 }
        val time = if (timePerTask != null) timePerTask * taskRunContexts.size else null

        appendSbatchParam(sbatchScript, "job-name", slurmWorkflowJobName)
        appendSbatchParam(sbatchScript, "output", logsPath.resolve("out.txt"))
        appendSbatchParam(sbatchScript, "error", logsPath.resolve("err.txt"))
        appendSbatchParam(sbatchScript, "mem", mem)
        appendSbatchParam(sbatchScript, "cpus-per-task", cpus)
        appendSbatchParam(sbatchScript, "time", time)
        appendSbatchParam(sbatchScript, "partition", taskConfig.slurm?.partition)
        for ((argName, argVal) in taskConfig.slurm?.sbatchArgs ?: mapOf()) {
            appendSbatchParam(sbatchScript, argName, argVal)
        }

        sbatchScript.append("\n")
        sbatchScript.append("set -x\n")

        for (taskRunContext in taskRunContexts) {
            val taskUUID = randomUUID().toString()

            // Create a temp directory to use as a mount for input data
            val mountDir = "/tmp/task-$taskUUID-mount"
            val mountTmpDir = "$mountDir/tmp"

            // Manually override singularity runtime dir to work-around deletion issue
            // https://github.com/sylabs/singularity/issues/1255
            val singularityRuntimeDir = "/tmp/singularity-$taskUUID"

            sbatchScript.append("\n")
            sbatchScript.append("### SubTask taskUUID ###")
            sbatchScript.append("trap 'rm -rf $mountDir; rm -rf $singularityRuntimeDir' EXIT\n")

            val tmpDir = Paths.get(taskRunContext.dockerDataDir, "tmp")
            sbatchScript.append("export MOUNT_DIR=$mountDir\n")
            sbatchScript.append("export SINGULARITY_LOCALCACHEDIR=$singularityRuntimeDir\n")
            sbatchScript.append("export SINGULARITY_BINDPATH=\$MOUNT_DIR:${taskRunContext.dockerDataDir}\n")
            sbatchScript.append("export SINGULARITYENV_TMP_DIR=$tmpDir\n")

            sbatchScript.append("\n")
            sbatchScript.append("# Create Singularity Local Cache Dir\n")
            sbatchScript.append("mkdir $singularityRuntimeDir\n")

            val inputFiles = taskRunContext.inputFiles
            val outputFilesIn = taskRunContext.outputFilesIn
            val outputFilesOut = taskRunContext.outputFilesOut

            // Add copying local input files into mounted dir to script.
            val localCopyInputFiles = inputFiles.filterIsInstance<LocalInputFile>()
            for (localCopyInputFile in localCopyInputFiles) {
                val mountDirFilePath = "$mountDir/${localCopyInputFile.path}"
                sbatchScript.append(copyCommand(localCopyInputFile.localPath, mountDirFilePath))
            }

            val remoteDownloadInputFiles = inputFiles.filter { it !is LocalInputFile }
            if (remoteDownloadInputFiles.isNotEmpty()) {
                sbatchScript.append("\n")
                sbatchScript.append("# Download files to mounted directory using singularity.\n")
            }
            for (remoteDownloadInputFile in remoteDownloadInputFiles) {
                val downloadCommand = remoteDownloadInputFile.downloadFileCommand(taskRunContext.dockerDataDir)
                sbatchScript.append("singularity exec --containall " +
                        "docker://${remoteDownloadInputFile.downloadFileImage()} $downloadCommand\n")
                sbatchScript.append("echo $?\n")
            }

            // Copy OutputFiles from task input into the docker container
            if (!outputFilesIn.isEmpty()) {
                log.info { "Copying output files $outputFilesIn from task input into mounted working dir for singularity: $mountDir" }
                sbatchScript.append("\n")
                sbatchScript.append("# Copy output files from previous tasks into mounted singularity data directory.\n")
            }
            for (outputFile in outputFilesIn) {
                val fromPath = outputsPath.resolve(outputFile.path)
                val mountDirFilePath = "$mountDir/${outputFile.path}"
                sbatchScript.append(copyCommand(fromPath.toString(), mountDirFilePath))
            }
            sbatchScript.append("\n")

            // Copy the run command into a sh file
            if (taskRunContext.command != null) {
                sbatchScript.append("mkdir -p $mountTmpDir\n")
                val runScriptContents = "set -e\n${taskRunContext.command}"
                val runScriptAsBase64 = Base64.getEncoder().encodeToString(runScriptContents.toByteArray())
                sbatchScript.append("echo $runScriptAsBase64 | base64 --decode > $mountTmpDir/$RUN_SCRIPT_NAME\n")
            }

            // Add running the task to script
            sbatchScript.append("\n")
            sbatchScript.append("# Run task command.\n")
            sbatchScript.append("singularity exec --containall docker://${taskRunContext.dockerImage}")
            if (taskRunContext.command != null) sbatchScript.append(" /bin/sh $tmpDir/$RUN_SCRIPT_NAME")
            sbatchScript.append("\n")

            // Add copying output files into output dir to script
            if (outputFilesOut.isNotEmpty()) {
                sbatchScript.append("\n")
                sbatchScript.append("# Copy output files out of mounted directory.\n")
            }
            for (outputFile in outputFilesOut) {
                val cachedFilePath = outputsPath.resolve(outputFile.path)
                val mountDirFilePath = "$mountDir/${outputFile.path}"
                sbatchScript.append(copyCommand(mountDirFilePath, cachedFilePath.toString()))
            }
        }

        val sbatchScriptAsBase64 = Base64.getEncoder().encodeToString(sbatchScript.toString().toByteArray())
        // Introduce random delay to spread out jobs.
        delay(kotlin.random.Random.nextLong(0, 5000))

        val sbatchCommand = "echo $sbatchScriptAsBase64 | base64 --decode | sbatch"
        val sbatchResponse = commandExecutor.exec(sbatchCommand)
        val jobId = "\\d+\$".toRegex().find(sbatchResponse)?.value
            ?: throw Exception("JobID not found in response for sbatch command")

        log.info { "Job ID $jobId found for sbatch command" }

        // Wait until status
        do {
            var done = false
            delay(workflowConfig.slurm!!.jobCompletionPollInterval * 1000L)

            retrySuspend("Slurm status check for job $jobId", 4, { it is SlurmCheckEmptyResponseException }) { attempt ->
                // Exponential backoff
                if (attempt > 1) {
                    val sleepTime = 2.0.pow(attempt).toLong()
                    log.info { "Empty sacct response. Sleeping for $sleepTime seconds." }
                    delay(1000 * sleepTime)
                }
                val checkCommand = "sacct -j $jobId --format=state --noheader"
                val jobStatusResponse = commandExecutor.exec(checkCommand)
                if (jobStatusResponse.isBlank()) throw SlurmCheckEmptyResponseException()
                val rawJobStatus = jobStatusResponse.split("\n")[0]
                    .trim().trimEnd('+')
                val jobStatus = SlurmJobState.valueOf(rawJobStatus)
                when (jobStatus.category) {
                    SlurmJobStateCategory.INCOMPLETE -> log.info { "Job $jobId still in progress with status $jobStatus" }
                    SlurmJobStateCategory.FAILED -> throw Exception("Job $jobId failed with status $jobStatus")
                    SlurmJobStateCategory.SUCCEEDED -> done = true
                }
            }
        } while (!done)
        log.info { "Job $jobId complete!" }
    }

    override fun shutdownRunningTasks() {
        allShutdown.set(true)
        commandExecutor.exec("scancel --jobname=$slurmWorkflowJobName")
    }

}

class SlurmCheckEmptyResponseException : Exception("Empty response given for Slurm job status lookup.")

/**
 * Utility to append SBATCH line to sbatch script if the given value is not null
 */
private fun appendSbatchParam(sbatchScript: StringBuilder, paramName: String, value: Any?) {
    if (value != null) sbatchScript.append("#SBATCH --$paramName=$value\n")
}

/**
 * Utility function to create a copy command that also creates any parent directories that don't already exist.
 */
private fun copyCommand(from: String, to: String) = "mkdir -p $(dirname $to) && cp $from $to\n"

enum class SlurmJobState(val category: SlurmJobStateCategory) {
    BOOT_FAIL(SlurmJobStateCategory.FAILED),
    CANCELLED(SlurmJobStateCategory.FAILED),
    COMPLETED(SlurmJobStateCategory.SUCCEEDED),
    DEADLINE(SlurmJobStateCategory.FAILED),
    FAILED(SlurmJobStateCategory.FAILED),
    NODE_FAIL(SlurmJobStateCategory.FAILED),
    OUT_OF_MEMORY(SlurmJobStateCategory.FAILED),
    PENDING(SlurmJobStateCategory.INCOMPLETE),
    PREEMPTED(SlurmJobStateCategory.FAILED),
    RUNNING(SlurmJobStateCategory.INCOMPLETE),
    REQUEUED(SlurmJobStateCategory.INCOMPLETE),
    RESIZING(SlurmJobStateCategory.INCOMPLETE),
    REVOKED(SlurmJobStateCategory.FAILED),
    SUSPENDED(SlurmJobStateCategory.INCOMPLETE),
    TIMEOUT(SlurmJobStateCategory.FAILED)
}

enum class SlurmJobStateCategory {
    INCOMPLETE, SUCCEEDED, FAILED
}