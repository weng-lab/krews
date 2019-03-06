package krews.executor.slurm

import krews.config.*
import krews.core.Task
import krews.core.TaskRunContext
import krews.core.TaskRunner
import krews.executor.*
import krews.file.*
import mu.KotlinLogging
import java.nio.file.*
import java.util.*
import java.util.UUID.randomUUID
import krews.misc.CommandExecutor
import org.apache.commons.io.FileUtils
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.pow

private val log = KotlinLogging.logger {}

const val RUN_SCRIPT_NAME = "run_script.sh"

class SlurmExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    // Slurm "Job Name" that will serve as an identifier for all jobs created by this workflow.
    // We will use this to delete jobs without needing to track individual job ids.
    private val slurmWorkflowJobName = randomUUID().toString()

    private val commandExecutor = CommandExecutor(workflowConfig.slurm.ssh)
    private val workflowBasePath = Paths.get(workflowConfig.localFilesBaseDir).toAbsolutePath()!!
    private val inputsPath = workflowBasePath.resolve(INPUTS_DIR)
    private val outputsPath = workflowBasePath.resolve(OUTPUTS_DIR)

    private var allShutdown = false

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

    override fun downloadInputFile(inputFile: InputFile) = downloadInputFileLocalFS(inputFile, inputsPath)

    private inner class SlurmJob(val jobId: String) : Future<Unit> {
        private var capturedThrowable: Throwable? = null
        private var lastStatus: SlurmJobStateCategory? = null
        private var lastCheckTime: Long = 0
        private var checkAttempt = 0
        private var cancelled = false


        private fun checkStatus() {
            if (lastStatus == SlurmJobStateCategory.SUCCEEDED || lastStatus == SlurmJobStateCategory.FAILED) {
                return
            }
            if (checkAttempt > 4) {
                // Force a failed state
                lastStatus = SlurmJobStateCategory.FAILED
                capturedThrowable = SlurmCheckEmptyResponseException()
                return
            }
            if (checkAttempt > 1) {
                val sleepTime = 2.0.pow(checkAttempt).toLong()
                if (System.currentTimeMillis() - lastCheckTime < sleepTime) {
                    return
                }
            }
            try {
                lastCheckTime = System.currentTimeMillis()
                val checkCommand = "sacct -j $jobId --format=state --noheader"
                val jobStatusResponse = commandExecutor.exec(checkCommand)
                if (jobStatusResponse.isBlank()) throw SlurmCheckEmptyResponseException()
                val rawJobStatus = jobStatusResponse.split("\n")[0]
                    .trim().trimEnd('+')
                val jobStatus = SlurmJobState.valueOf(rawJobStatus)
                when (jobStatus.category) {
                    SlurmJobStateCategory.INCOMPLETE -> log.info { "Job $jobId still in progress with status $jobStatus" }
                    SlurmJobStateCategory.FAILED -> log.info { "Job $jobId failed with status $jobStatus" }
                    SlurmJobStateCategory.SUCCEEDED -> Unit
                }
                lastStatus = jobStatus.category
                checkAttempt = 0
                if (lastStatus == SlurmJobStateCategory.SUCCEEDED) {
                    log.info { "Job $jobId complete!" }
                }
            } catch (e: SlurmCheckEmptyResponseException) {
                checkAttempt += 1
                val sleepTime = 2.0.pow(checkAttempt).toLong()
                log.info { "Empty sacct response. Sleeping for $sleepTime seconds before next attempt." }
            } catch (e: Throwable) {
                lastStatus = SlurmJobStateCategory.FAILED
                capturedThrowable = e
            }
        }

        private fun onFinished(success: Boolean) {}

        override fun isDone(): Boolean {
            checkStatus()
            return capturedThrowable != null || (lastStatus == SlurmJobStateCategory.SUCCEEDED || lastStatus == SlurmJobStateCategory.FAILED)
        }

        override fun get() {
            // An arbitrarily high value that won't overflow the long
            return get(30, TimeUnit.DAYS)
        }

        override fun get(timeout: Long, unit: TimeUnit?) {
            try {
                val startTime: Long = 0
                do {
                    if (capturedThrowable != null) {
                        throw capturedThrowable!!
                    }
                    val waitTime = TimeUnit.MILLISECONDS.convert(timeout, (unit ?: TimeUnit.MILLISECONDS))
                    if (startTime + waitTime > System.currentTimeMillis()) {
                        throw TimeoutException()
                    }

                    Thread.sleep(workflowConfig.slurm.jobCompletionPollInterval * 1000L)
                } while (!isDone)
            } finally {
                onFinished(lastStatus == SlurmJobStateCategory.SUCCEEDED)
            }
        }


        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            // First do quick status check if needed (don't block, yet)
            if (lastStatus == null) {
                checkStatus()
            }
            when (lastStatus) {
                null -> {
                    if (mayInterruptIfRunning) {
                        // This does not exact match the spec. We assume it is running, send cancel command, and always return true
                        commandExecutor.exec("scancel --jobid=$jobId")
                        cancelled = true
                        return true
                    }
                    // Block
                    get()
                    return cancel(mayInterruptIfRunning)
                }
                SlurmJobStateCategory.SUCCEEDED -> return false
                SlurmJobStateCategory.FAILED -> return false
                SlurmJobStateCategory.INCOMPLETE -> {
                    if (mayInterruptIfRunning) {
                        // This does not exact match the spec. We assume it is still running, send cancel command, and always return true
                        commandExecutor.exec("scancel --jobid=$jobId")
                        cancelled = true
                        return true
                    }
                    return false
                }
            }
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
        val runBasePath = workflowBasePath.resolve(workflowRunDir)
        val logsPath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())
        logsPath.toFile().mkdirs()

        val taskUUID = randomUUID().toString()

        // Create a temp directory to use as a mount for input data
        val mountDir = "/tmp/task-$taskUUID-mount"
        val mountTmpDir = "$mountDir/tmp"

        // Manually override singularity runtime dir to work-around deletion issue
        // https://github.com/sylabs/singularity/issues/1255
        val singularityRuntimeDir = "/tmp/singularity-$taskUUID"

        try {
            val sbatchScript = StringBuilder("#!/bin/bash\n#\n")

            appendSbatchParam(sbatchScript, "job-name", slurmWorkflowJobName)
            appendSbatchParam(sbatchScript, "output", logsPath.resolve("out.txt"))
            appendSbatchParam(sbatchScript, "error", logsPath.resolve("err.txt"))
            appendSbatchParam(sbatchScript, "mem", taskConfig.slurm?.mem ?: taskRunContext.memory)
            appendSbatchParam(sbatchScript, "cpus-per-task", taskConfig.slurm?.cpus ?: taskRunContext.cpus)
            appendSbatchParam(sbatchScript, "time", taskConfig.slurm?.time ?: taskRunContext.time)
            appendSbatchParam(sbatchScript, "partition", taskConfig.slurm?.partition)
            for ((argName, argVal) in taskConfig.slurm?.sbatchArgs ?: mapOf()) {
                appendSbatchParam(sbatchScript, argName, argVal)
            }

            val tmpDir = Paths.get(taskRunContext.dockerDataDir, "tmp")

            sbatchScript.append("\n")
            sbatchScript.append("set -x\n")
            sbatchScript.append("trap 'rm -rf $mountDir; rm -rf $singularityRuntimeDir' EXIT\n")
            sbatchScript.append("export MOUNT_DIR=$mountDir\n")
            sbatchScript.append("export SINGULARITY_LOCALCACHEDIR=$singularityRuntimeDir\n")
            sbatchScript.append("export SINGULARITY_BINDPATH=\$MOUNT_DIR:${taskRunContext.dockerDataDir}\n")
            sbatchScript.append("export SINGULARITYENV_TMP_DIR=$tmpDir\n")

            sbatchScript.append("\n")
            sbatchScript.append("# Create Singularity Local Cache Dir\n")
            sbatchScript.append("mkdir $singularityRuntimeDir\n")

            // Add downloading cached input files into mounted dir to script
            if (cachedInputFiles.isNotEmpty()) {
                sbatchScript.append("\n")
                sbatchScript.append("# Copy local files needed for job into mounted singularity data directory.\n")
            }
            for (cachedInputFile in cachedInputFiles) {
                val cachedFilePath = Paths.get(workflowBasePath.toString(), INPUTS_DIR, cachedInputFile.path)
                val mountDirFilePath = "$mountDir/${cachedInputFile.path}"
                sbatchScript.append(copyCommand(cachedFilePath.toString(), mountDirFilePath))
            }

            // Add copying local non-cached input files into mounted dir to script.
            val localCopyInputFiles = downloadInputFiles.filterIsInstance<LocalInputFile>()
            for (localCopyInputFile in localCopyInputFiles) {
                val mountDirFilePath = "$mountDir/${localCopyInputFile.path}"
                sbatchScript.append(copyCommand(localCopyInputFile.localPath, mountDirFilePath))
            }

            val remoteDownloadInputFiles = downloadInputFiles.filter { it !is LocalInputFile }
            if (remoteDownloadInputFiles.isNotEmpty()) {
                sbatchScript.append("\n")
                sbatchScript.append("# Download files to mounted directory using singularity.\n")
            }
            for (remoteDownloadInputFile in remoteDownloadInputFiles) {
                val downloadCommand = remoteDownloadInputFile.downloadFileCommand(taskRunContext.dockerDataDir)
                sbatchScript.append("singularity exec --containall docker://${remoteDownloadInputFile.downloadFileImage()} $downloadCommand\n")
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

            val sbatchScriptAsBase64 = Base64.getEncoder().encodeToString(sbatchScript.toString().toByteArray())
            val sbatchCommand = "sleep ${kotlin.random.Random.nextDouble(0.0, 5.0)}; " +
                    "echo $sbatchScriptAsBase64 | base64 --decode | sbatch"
            val sbatchResponse = commandExecutor.exec(sbatchCommand)
            val jobId = "\\d+\$".toRegex().find(sbatchResponse)?.value
                ?: throw Exception("JobID not found in response for sbatch command")

            log.info { "Job ID $jobId found for sbatch command" }
            return SlurmJob(jobId)
        } catch(e: Throwable) {
            return CompletableFuture.failedFuture(e)
        }
    }

    override fun shutdownRunningTasks() {
        allShutdown = true
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