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
import kotlin.streams.*

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

    //private val httpInputDockerImage = workflowConfig.slurm?.httpInputDockerImage

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

    override fun uploadFile(fromPath: Path, toPath: String) {
        val toFile = workflowBasePath.resolve(toPath)
        log.info { "Copying file $fromPath to $toFile" }
        Files.createDirectories(toFile.parent)
        FileUtils.copyFile(fromPath.toFile(), toFile.toFile())
    }

    override fun fileExists(path: String): Boolean = Files.exists(workflowBasePath.resolve(path))

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

        // ***NOTE***
        // We manually override singularity runtime dir to work-around deletion issue
        // https://github.com/sylabs/singularity/issues/1255

        val taskUUID = randomUUID().toString()

        // Begin by setting up the temporary task folder
        // This has these directories: outputs, downloaded, tmp, singularities
        // the singularity tmp directory is inside the outputs directory

        // Create a temp directory to use as a mount for input data
        val mountDir = "/tmp/task-$taskUUID-mount"
        val mountOuputsDir = "$mountDir/outputs"
        val mountDownloadedDir = "$mountDir/downloaded"
        val mountTmpDir = "$mountDir/tmp"
        val mountSingularitiesDir = "$mountDir/singularities"

        // Ensure we cleanup after ourselves on exit
        sbatchScript.append("trap 'rm -rf $mountDir;' EXIT\n")

        sbatchScript.append("mkdir -p $mountOuputsDir\n")
        sbatchScript.append("mkdir -p $mountDownloadedDir\n")
        sbatchScript.append("mkdir -p $mountTmpDir\n")
        sbatchScript.append("mkdir -p $mountSingularitiesDir\n")


        // First download the set of all remote input files into downloaded
        val remoteDownloadInputFiles = taskRunContexts.flatMap { trc -> trc.inputFiles.filter { it !is LocalInputFile } }.toSet()
        if (remoteDownloadInputFiles.isNotEmpty()) {
            sbatchScript.append("\n")
            sbatchScript.append("# Download files to mounted directory using singularity.\n")

            // TODO: could eventually be changed to download all files in one container instance
            for (remoteDownloadInputFile in remoteDownloadInputFiles) {
                val singUUID = randomUUID().toString()
                sbatchScript.append("\n")
                sbatchScript.append("### Download $singUUID ###\n")

                val singularityRuntimeDir = "$mountSingularitiesDir/singularity-$singUUID"

                sbatchScript.append("export SINGULARITY_LOCALCACHEDIR=$singularityRuntimeDir\n")
                sbatchScript.append("export SINGULARITY_BINDPATH=$mountDownloadedDir:/download\n")

                val downloadCommand = remoteDownloadInputFile.downloadFileCommand("/download")
                var downloadImageName = remoteDownloadInputFile.downloadFileImage()

                /*if(httpInputDockerImage != null)
                {
                    downloadImageName= httpInputDockerImage
                }*/
                
                sbatchScript.append("singularity exec --containall docker://$downloadImageName $downloadCommand\n")
                sbatchScript.append("echo Exit status of download $singUUID: $?\n")
            }
        }

        for (taskRunContext in taskRunContexts) {
            val singUUID = randomUUID().toString()
            sbatchScript.append("\n")
            sbatchScript.append("### SubTask $singUUID ###\n")

            // Copy the run command into a sh file
            val (containerCommand, scriptBind) =
                if (taskRunContext.command != null) {
                    val runScriptContents = "set -e\n${taskRunContext.command}"
                    val runScriptAsBase64 = Base64.getEncoder().encodeToString(runScriptContents.toByteArray())
                    sbatchScript.append("echo $runScriptAsBase64 | base64 --decode > $mountTmpDir/$singUUID-script.sh\n")
                    Pair("/bin/sh ${taskRunContext.inputsDir}/$RUN_SCRIPT_NAME", "$mountTmpDir/$singUUID-script.sh:${taskRunContext.inputsDir}/$RUN_SCRIPT_NAME:ro")
                } else {
                    Pair("", null)
                }

            val singularityRuntimeDir = "$mountSingularitiesDir/singularity-$singUUID"
            sbatchScript.append("export SINGULARITY_LOCALCACHEDIR=$singularityRuntimeDir\n")

            val inputFiles = taskRunContext.inputFiles
            val outputFilesIn = taskRunContext.outputFilesIn
            val tmpBind = "$mountTmpDir:/tmp"
            val outputsBind = "$mountOuputsDir:${taskRunContext.outputsDir}"
            val localInputsBind = inputFiles.filterIsInstance<LocalInputFile>().joinToString(",") { "${it.localPath}:${taskRunContext.inputsDir}/${it.path}:ro" }
            val remoteInputsBind = inputFiles.filter { it !is LocalInputFile }.joinToString(",") { "$mountDownloadedDir/${it.path}:${taskRunContext.inputsDir}/${it.path}:ro" }
            val outputFilesInBinds = outputFilesIn.joinToString(",") { "${outputsPath.resolve(it.path)}:${taskRunContext.inputsDir}/${it.path}:ro" }
            val binds = listOfNotNull(tmpBind, outputsBind, localInputsBind, remoteInputsBind, outputFilesInBinds, scriptBind)
                .filter { it.isNotEmpty() }.joinToString(",")
            sbatchScript.append("export SINGULARITY_BIND=\"$binds\"\n")

            // Add running the task to script
            sbatchScript.append("\n")
            sbatchScript.append("# Run task command.\n")
            sbatchScript.append("singularity exec --containall docker://${taskRunContext.dockerImage} $containerCommand")
            sbatchScript.append("\n")

            // Add copying output files into output dir to script
            val outputFilesOut = taskRunContext.outputFilesOut
            if (outputFilesOut.isNotEmpty()) {
                sbatchScript.append("\n")
                sbatchScript.append("# Copy output files out of mounted directory.\n")

                for (outputFile in outputFilesOut) {
                    val outFilePath = outputsPath.resolve(outputFile.path)
                    val mountDirFilePath = "$mountOuputsDir/${outputFile.path}"
                    val presentCopyCmd = copyCommand(mountDirFilePath, outFilePath.toString())
                    val copyCmd = if (outputFile.optional) {
                        val noneFilePath = "$outFilePath.$NONE_SUFFIX"
                        val missingCopyCmd = "mkdir -p \$(dirname $noneFilePath) && touch $noneFilePath"
                        "if [ -f $mountDirFilePath ]; then $presentCopyCmd; else $missingCopyCmd; fi"
                    } else presentCopyCmd
                    sbatchScript.append("${copyCmd}\n")
                }
            }

            val outputDirectoriesOut = taskRunContext.outputDirectoriesOut
            // Copy output directories out of docker container into run outputs dir
            if (outputDirectoriesOut.isNotEmpty()) {
                sbatchScript.append("\n")
                sbatchScript.append("# Copy output directories out of mounted directory.\n")

                for (outputDirectory in outputDirectoriesOut) {
                    val outDirectoryPath = outputsPath.resolve(outputDirectory.path)
                    val mountDirFilePath = "$mountOuputsDir/${outputDirectory.path}"
                    val copyCmd = copyRecursiveOverwriteCommand(mountDirFilePath, outDirectoryPath.toString())
                    sbatchScript.append("${copyCmd}\n")
                }
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
            delay((workflowConfig.slurm?.jobCompletionPollInterval ?: 10) * 1000L)

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

        for (taskRunContext in taskRunContexts) {
            taskRunContext.outputDirectoriesOut.forEach { outDir ->
                val to = outputsPath.resolve(outDir.path)

                outDir.filesFuture.complete(
                    Files
                        .walk(to)
                        .asSequence()
                        .filter { !Files.isDirectory(it) }
                        .map { outputsPath.relativize(it) }
                        .map { OutputFile(it.toString())}
                        .toList()
                )
            }
        }

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
private fun copyCommand(from: String, to: String) = "mkdir -p $(dirname $to) && cp $from $to"

/**
 * Utility function to create a copy command that also creates any parent directories that don't already exist.
 */
private fun copyRecursiveOverwriteCommand(from: String, to: String) = "mkdir -p $(dirname $to) && cp -rf $from/* $to"

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