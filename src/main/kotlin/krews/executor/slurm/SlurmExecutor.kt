package krews.executor.slurm

import krews.config.*
import krews.core.TaskRunContext
import krews.executor.*
import krews.file.*
import mu.KotlinLogging
import java.nio.file.*
import java.util.*
import java.util.UUID.randomUUID
import krews.misc.CommandExecutor
import java.io.FileOutputStream

private val log = KotlinLogging.logger {}

const val RUN_SCRIPT_NAME = "run_script.sh"
const val SBATCH_SCRIPT_NAME = "sbatch_script.sh"

class SlurmExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    // Slurm "Job Name" that will serve as an identifier for all jobs created by this workflow.
    // We will use this to delete jobs without needing to track individual job ids.
    private val slurmWorkflowJobName = randomUUID().toString()

    private val commandExecutor = CommandExecutor(workflowConfig.slurm.ssh)
    private val workflowBasePath = Paths.get(workflowConfig.localFilesBaseDir).toAbsolutePath()!!
    private val inputsPath = workflowBasePath.resolve(INPUTS_DIR)
    private val outputsPath = workflowBasePath.resolve(OUTPUTS_DIR)

    override fun downloadFile(path: String) {}
    override fun uploadFile(fromPath: String, toPath: String, backup: Boolean) {}

    override fun fileExists(path: String): Boolean = Files.exists(workflowBasePath.resolve(path))
    override fun fileLastModified(path: String): Long = Files.getLastModifiedTime(workflowBasePath.resolve(path)).toMillis()

    override fun listFiles(baseDir: String): Set<String> = listLocalFiles(workflowBasePath.resolve(baseDir))
    override fun deleteFile(file: String) = Files.delete(workflowBasePath.resolve(file))

    override fun downloadInputFile(inputFile: InputFile) = downloadInputFileLocalFS(inputFile, inputsPath)

    override fun executeTask(
        workflowRunDir: String,
        taskRunId: Int,
        taskConfig: TaskConfig,
        taskRunContext: TaskRunContext<*, *>,
        outputFilesIn: Set<OutputFile>,
        outputFilesOut: Set<OutputFile>,
        cachedInputFiles: Set<InputFile>,
        downloadInputFiles: Set<InputFile>
    ) {
        val runBasePath = workflowBasePath.resolve(workflowRunDir)
        val logsPath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())
        logsPath.toFile().mkdirs()

        // Create a temp directory to use as a mount for input data
        val mountDir = workflowBasePath.resolve("task-$taskRunId-mount")
        val mountTmpDir = mountDir.resolve("tmp")

        log.info { "Creating working directory $mountDir for task run $taskRunId" }
        mountDir.toFile().mkdirs()
        mountTmpDir.toFile().mkdirs()

        try {
            val sbatchScript = StringBuilder("#!/bin/bash\n#\n")

            appendSbatchParam(sbatchScript, "job-name", slurmWorkflowJobName)
            appendSbatchParam(sbatchScript, "output", logsPath.resolve("out.txt"))
            appendSbatchParam(sbatchScript, "error", logsPath.resolve("err.txt"))
            appendSbatchParam(sbatchScript, "mem", taskConfig.slurm?.mem ?: taskRunContext.memory)
            appendSbatchParam(sbatchScript, "cpus-per-task", taskConfig.slurm?.cpus ?: taskRunContext.cpus)
            appendSbatchParam(sbatchScript, "time", taskConfig.slurm?.time ?: taskRunContext.time)
            appendSbatchParam(sbatchScript, "partition", taskConfig.slurm?.partition)

            val tmpDir = Paths.get(taskRunContext.dockerDataDir, "tmp")

            sbatchScript.append("\nexport SINGULARITY_BINDPATH=$mountDir:${taskRunContext.dockerDataDir}\n")
            sbatchScript.append("export SINGULARITYENV_TMP_DIR=$tmpDir\n")

            // Add downloading cached input files into mounted dir to script
            if (cachedInputFiles.isNotEmpty()) {
                sbatchScript.append("\n# Copy local files needed for job into mounted singularity data directory.\n")
            }
            for (cachedInputFile in cachedInputFiles) {
                val cachedFilePath = Paths.get(workflowBasePath.toString(), INPUTS_DIR, cachedInputFile.path)
                val mountDirFilePath = mountDir.resolve(cachedInputFile.path)
                sbatchScript.append(copyCommand(cachedFilePath.toString(), mountDirFilePath.toString()))
            }

            // Add copying local non-cached input files into mounted dir to script.
            val localCopyInputFiles = downloadInputFiles.filterIsInstance<LocalInputFile>()
            for (localCopyInputFile in localCopyInputFiles) {
                val mountDirFilePath = mountDir.resolve(localCopyInputFile.path)
                sbatchScript.append(copyCommand(localCopyInputFile.localPath, mountDirFilePath.toString()))
            }

            val remoteDownloadInputFiles = downloadInputFiles.filter { it !is LocalInputFile }
            if (remoteDownloadInputFiles.isNotEmpty()) {
                sbatchScript.append("\n# Download files to mounted directory using singularity.\n")
            }
            for (remoteDownloadInputFile in remoteDownloadInputFiles) {
                val downloadCommand = remoteDownloadInputFile.downloadFileCommand(taskRunContext.dockerDataDir)
                sbatchScript.append("singularity exec docker://${remoteDownloadInputFile.downloadFileImage()} $downloadCommand\n")
                sbatchScript.append("echo $?\n")
            }

            // Copy OutputFiles from task input into the docker container
            if (!outputFilesIn.isEmpty()) {
                log.info { "Copying output files $outputFilesIn from task input into mounted working dir for singularity: $mountDir" }
                sbatchScript.append("\n# Copy output files from previous tasks into mounted singularity data directory.\n")
            }
            for (outputFile in outputFilesIn) {
                val fromPath = outputsPath.resolve(outputFile.path)
                val mountDirFilePath = mountDir.resolve(outputFile.path)
                sbatchScript.append(copyCommand(fromPath.toString(), mountDirFilePath.toString()))
            }

            // Copy the run command into a sh file
            if (taskRunContext.command != null) {
                val runScriptContents = "set -e\n${taskRunContext.command}"
                val taskRunFile = mountTmpDir.resolve(RUN_SCRIPT_NAME).toString()
                log.info { "Creating singularity command script file $taskRunFile with content:\n$runScriptContents" }
                FileOutputStream(taskRunFile).use { it.write(runScriptContents.toByteArray()) }
            }

            // Add running the task to script
            sbatchScript.append("\n# Run task command.\n")
            sbatchScript.append("singularity exec docker://${taskRunContext.dockerImage}")
            if (taskRunContext.command != null) sbatchScript.append(" /bin/sh ${tmpDir.resolve(RUN_SCRIPT_NAME)}")
            sbatchScript.append("\n")

            // Add copying output files into output dir to script
            if (outputFilesOut.isNotEmpty()) {
                sbatchScript.append("\n# Copy output files out of mounted directory.\n")
            }
            for (outputFile in outputFilesOut) {
                val cachedFilePath = outputsPath.resolve(outputFile.path)
                val mountDirFilePath = mountDir.resolve(outputFile.path)
                sbatchScript.append(copyCommand(mountDirFilePath.toString(), cachedFilePath.toString()))
            }

            val sbatchFile = mountTmpDir.resolve(SBATCH_SCRIPT_NAME).toString()
            FileOutputStream(sbatchFile).use { sbatchScript.toString().toByteArray() }
            log.info { "Created sbatch script file $sbatchFile with content:\n$sbatchScript" }
            val sbatchCommand = "sbatch $sbatchFile"
            val sbatchResponse = commandExecutor.exec(sbatchCommand)
            val jobId = "\\d+\$".toRegex().find(sbatchResponse)?.value ?:
                throw Exception("JobID not found in response for sbatch command")

            log.info { "Job ID $jobId found for sbatch command" }

            // Wait until status
            do {
                var done = false
                Thread.sleep(workflowConfig.slurm.jobCompletionPollInterval * 1000L)

                val checkCommand = "sacct -j $jobId --format=state --noheader"
                val jobStatusResponse = commandExecutor.exec(checkCommand)
                if (jobStatusResponse.isBlank()) throw Exception("Empty response given for Slurm job status lookup.")
                val jobStatus = SlurmJobState.valueOf(jobStatusResponse.split("\n")[0].trim())
                when (jobStatus.category) {
                    SlurmJobStateCategory.INCOMPLETE -> log.info { "Job $jobId still in progress with status $jobStatus" }
                    SlurmJobStateCategory.FAILED -> throw Exception("Job $jobId failed with status $jobStatus")
                    SlurmJobStateCategory.SUCCEEDED -> done = true
                }
            } while(!done)
            log.info { "Job $jobId complete!" }
        } finally {
            // Clean up temporary mount dir
            Files.walk(mountDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.delete(it) }
        }
    }

    override fun shutdownRunningTasks() {
        commandExecutor.exec("scancel --jobname=$slurmWorkflowJobName")
    }

}

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