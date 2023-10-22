package krews.executor.bsub

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

class BsubExecutor(private val workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    // BSUB "Job Name" that will serve as an identifier for all jobs created by this workflow.
    // We will use this to delete jobs without needing to track individual job ids.
    private val bsubWorkflowJobName = randomUUID().toString()

    private val commandExecutor = CommandExecutor(workflowConfig.bsub?.ssh)
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

        val bsubScript = StringBuilder("#!/bin/bash\n#\n")

        val mem = taskConfig.bsub?.mem ?: taskRunContexts.map { it.memory }.maxBy { it?.bytes ?: -1 }
        val cpus = taskConfig.bsub?.cpus ?: taskRunContexts.map { it.cpus }.maxBy { it ?: -1 }
        val gpus = if (taskConfig.bsub?.gpu == true) "\"\"" else null

        appendBsubParam(bsubScript, "J", bsubWorkflowJobName)
        appendBsubParam(bsubScript, "o", logsPath.resolve("out.txt"))
        appendBsubParam(bsubScript, "e", logsPath.resolve("err.txt"))
        appendBsubParam(bsubScript, "R", taskConfig.bsub?.rUsage)
        appendBsubParam(bsubScript, "n", cpus)
        appendBsubParam(bsubScript, "W", taskConfig.bsub?.time)
        appendBsubParam(bsubScript, "q", taskConfig.bsub?.partition)
        appendBsubParam(bsubScript, "gpu", gpus)
        for ((argName, argVal) in taskConfig.bsub?.sbatchArgs ?: mapOf()) {
            appendBsubParam(bsubScript, argName, argVal)
        }

        bsubScript.append("\n")
        bsubScript.append("set -x\n")

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
        bsubScript.append("trap 'rm -rf $mountDir;' EXIT\n")

        bsubScript.append("mkdir -p $mountOuputsDir\n")
        bsubScript.append("mkdir -p $mountDownloadedDir\n")
        bsubScript.append("mkdir -p $mountTmpDir\n")
        bsubScript.append("mkdir -p $mountSingularitiesDir\n")


        // First download the set of all remote input files into downloaded
        val remoteDownloadInputFiles = taskRunContexts.flatMap { trc -> trc.inputFiles.filter { it !is LocalInputFile } }.toSet()
        if (remoteDownloadInputFiles.isNotEmpty()) {
            bsubScript.append("\n")
            bsubScript.append("# Download files to mounted directory using singularity.\n")

            // TODO: could eventually be changed to download all files in one container instance
            for (remoteDownloadInputFile in remoteDownloadInputFiles) {
                val singUUID = randomUUID().toString()
                bsubScript.append("\n")
                bsubScript.append("### Download $singUUID ###\n")

                val singularityRuntimeDir = "$mountSingularitiesDir/singularity-$singUUID"

                bsubScript.append("export SINGULARITY_LOCALCACHEDIR=$singularityRuntimeDir\n")
                bsubScript.append("export SINGULARITY_BINDPATH=$mountDownloadedDir:/download\n")

                val downloadCommand = remoteDownloadInputFile.downloadFileCommand("/download")
                var downloadImageName = remoteDownloadInputFile.downloadFileImage()

                if(remoteDownloadInputFile is GeoInputFile) {
                    var fileName = "${remoteDownloadInputFile.sraaccession}_${remoteDownloadInputFile.read}.fastq"
                    if(remoteDownloadInputFile.read === ReadCategory.READ_1) {
                        fileName = "${remoteDownloadInputFile.sraaccession}_${remoteDownloadInputFile.read}_1.fastq"
                    } else if(remoteDownloadInputFile.read === ReadCategory.READ_2) {
                        fileName = "${remoteDownloadInputFile.sraaccession}_${remoteDownloadInputFile.read}_2.fastq"
                    }

                    val renameFastqCommand = "mv /download/${fileName} /download/${remoteDownloadInputFile.path}"

                    // generate FASTQ file from SRA accession
                    bsubScript.append("singularity exec docker://$downloadImageName $downloadCommand\n")

                    // rename output file based on read
                    bsubScript.append("singularity exec docker://$downloadImageName $renameFastqCommand\n")

                } else {
                    bsubScript.append("singularity exec --containall docker://$downloadImageName $downloadCommand\n")
                }
                bsubScript.append("echo Exit status of download $singUUID: $?\n")
            }
        }

        for (taskRunContext in taskRunContexts) {
            val singUUID = randomUUID().toString()
            bsubScript.append("\n")
            bsubScript.append("### SubTask $singUUID ###\n")

            // Copy the run command into a sh file
            val (containerCommand, scriptBind) =
                if (taskRunContext.command != null) {
                    val runScriptContents = "set -e\n${taskRunContext.command}"
                    val runScriptAsBase64 = Base64.getEncoder().encodeToString(runScriptContents.toByteArray())
                    bsubScript.append("echo $runScriptAsBase64 | base64 --decode > $mountTmpDir/$singUUID-script.sh\n")
                    Pair("/bin/sh ${taskRunContext.inputsDir}/$RUN_SCRIPT_NAME", "$mountTmpDir/$singUUID-script.sh:${taskRunContext.inputsDir}/$RUN_SCRIPT_NAME:ro")
                } else {
                    Pair("", null)
                }

            val singularityRuntimeDir = "$mountSingularitiesDir/singularity-$singUUID"
            bsubScript.append("export SINGULARITY_LOCALCACHEDIR=$singularityRuntimeDir\n")

            val inputFiles = taskRunContext.inputFiles
            val outputFilesIn = taskRunContext.outputFilesIn
            val tmpBind = "$mountTmpDir:/tmp"
            val outputsBind = "$mountOuputsDir:${taskRunContext.outputsDir}"
            val localInputsBind = inputFiles.filterIsInstance<LocalInputFile>().joinToString(",") { "${it.localPath}:${taskRunContext.inputsDir}/${it.path}:ro" }
            val remoteInputsBind = inputFiles.filter { it !is LocalInputFile }.joinToString(",") { "$mountDownloadedDir/${it.path}:${taskRunContext.inputsDir}/${it.path}:ro" }
            val outputFilesInBinds = outputFilesIn.joinToString(",") { "${outputsPath.resolve(it.path)}:${taskRunContext.inputsDir}/${it.path}:ro" }
            val binds = listOfNotNull(tmpBind, outputsBind, localInputsBind, remoteInputsBind, outputFilesInBinds, scriptBind)
                .filter { it.isNotEmpty() }.joinToString(",")
            bsubScript.append("export SINGULARITY_BIND=\"$binds\"\n")

            // Add running the task to script
            bsubScript.append("\n")
            bsubScript.append("# Run task command.\n")
            bsubScript.append("singularity exec --containall docker://${taskRunContext.dockerImage} $containerCommand")
            bsubScript.append("\n")

            // Add copying output files into output dir to script
            val outputFilesOut = taskRunContext.outputFilesOut
            if (outputFilesOut.isNotEmpty()) {
                bsubScript.append("\n")
                bsubScript.append("# Copy output files out of mounted directory.\n")

                for (outputFile in outputFilesOut) {
                    val outFilePath = outputsPath.resolve(outputFile.path)
                    val mountDirFilePath = "$mountOuputsDir/${outputFile.path}"
                    val presentCopyCmd = copyCommand(mountDirFilePath, outFilePath.toString())
                    val copyCmd = if (outputFile.optional) {
                        val noneFilePath = "$outFilePath.$NONE_SUFFIX"
                        val missingCopyCmd = "mkdir -p \$(dirname $noneFilePath) && touch $noneFilePath"
                        "if [ -f $mountDirFilePath ]; then $presentCopyCmd; else $missingCopyCmd; fi"
                    } else presentCopyCmd
                    bsubScript.append("${copyCmd}\n")
                }
            }

            val outputDirectoriesOut = taskRunContext.outputDirectoriesOut
            // Copy output directories out of docker container into run outputs dir
            if (outputDirectoriesOut.isNotEmpty()) {
                bsubScript.append("\n")
                bsubScript.append("# Copy output directories out of mounted directory.\n")

                for (outputDirectory in outputDirectoriesOut) {
                    val outDirectoryPath = outputsPath.resolve(outputDirectory.path)
                    val mountDirFilePath = "$mountOuputsDir/${outputDirectory.path}"
                    val copyCmd = copyRecursiveOverwriteCommand(mountDirFilePath, outDirectoryPath.toString())
                    bsubScript.append("${copyCmd}\n")
                }
            }
        }

        val bsubScriptAsBase64 = Base64.getEncoder().encodeToString(bsubScript.toString().toByteArray())
        // Introduce random delay to spread out jobs.
        delay(kotlin.random.Random.nextLong(0, 5000))

        val bsubCommand = "echo $bsubScriptAsBase64 | base64 --decode | bsub"
        val bsubResponse = commandExecutor.exec(bsubCommand)
        val jobId = "\\d+\$".toRegex().find(bsubResponse)?.value
            ?: throw Exception("JobID not found in response for bsub command")

        log.info { "Job ID $jobId found for bsub command" }

        // Wait until status
        do {
            var done = false
            delay((workflowConfig.bsub?.jobCompletionPollInterval ?: 10) * 1000L)

            retrySuspend("BSUB status check for job $jobId", 4, { it is BsubCheckEmptyResponseException }) { attempt ->
                // Exponential backoff
                if (attempt > 1) {
                    val sleepTime = 2.0.pow(attempt).toLong()
                    log.info { "Empty bjobs response. Sleeping for $sleepTime seconds." }
                    delay(1000 * sleepTime)
                }
                val checkCommand = "bjobs -o stat -noheader $jobId"
                val jobStatusResponse = commandExecutor.exec(checkCommand)
                if (jobStatusResponse.isBlank()) throw BsubCheckEmptyResponseException()
                val rawJobStatus = jobStatusResponse.split("\n")[0]
                    .trim().trimEnd('+')
                val jobStatus = BsubJobState.valueOf(rawJobStatus)
                when (jobStatus.category) {
                    BsubJobStateCategory.INCOMPLETE -> log.info { "Job $jobId still in progress with status $jobStatus" }
                    BsubJobStateCategory.FAILED -> throw Exception("Job $jobId failed with status $jobStatus")
                    BsubJobStateCategory.SUCCEEDED -> done = true
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
        commandExecutor.exec("bkill -J $bsubWorkflowJobName")
    }

}

class BsubCheckEmptyResponseException : Exception("Empty response given for BSUB job status lookup.")

/**
 * Utility to append SBATCH line to sbatch script if the given value is not null
 */
private fun appendBsubParam(bsubScript: StringBuilder, paramName: String, value: Any?) {
    if (value != null) bsubScript.append("#BSUB -$paramName $value\n")
}

/**
 * Utility function to create a copy command that also creates any parent directories that don't already exist.
 */
private fun copyCommand(from: String, to: String) = "mkdir -p $(dirname $to) && cp $from $to"

/**
 * Utility function to create a copy command that also creates any parent directories that don't already exist.
 */
private fun copyRecursiveOverwriteCommand(from: String, to: String) = "mkdir -p $(dirname $to) && cp -rf $from/* $to"

enum class BsubJobState(val category: BsubJobStateCategory) {
    EXIT(BsubJobStateCategory.FAILED),
    DONE(BsubJobStateCategory.SUCCEEDED),
    PEND(BsubJobStateCategory.INCOMPLETE),
    PSUSP(BsubJobStateCategory.INCOMPLETE),
    USUSP(BsubJobStateCategory.INCOMPLETE),
    SSUSP(BsubJobStateCategory.INCOMPLETE),
    UNKWN(BsubJobStateCategory.FAILED)
}

enum class BsubJobStateCategory {
    INCOMPLETE, SUCCEEDED, FAILED
}
