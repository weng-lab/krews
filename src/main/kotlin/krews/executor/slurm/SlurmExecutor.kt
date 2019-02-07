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

private val log = KotlinLogging.logger {}

private val TMP_DIR = "tmp"

class SlurmExecutor(workflowConfig: WorkflowConfig) : LocallyDirectedExecutor {

    // Slurm "Job Name" that will serve as an identifier for all jobs created by this workflow.
    // We will use this to delete jobs without needing to track individual job ids.
    private val slurmWorkflowJobName = randomUUID().toString()

    private val slurmConfig = checkNotNull(workflowConfig.slurm)
        { "Slurm workflow config must be present to use Slurm Executor" }
    private val commandExecutor = CommandExecutor(slurmConfig.sshConfig)
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
        // Create a temp directory to use as a mount for input data
        val mountDir = workflowBasePath.resolve("task-$taskRunId-mount")
        Files.createDirectories(mountDir)

        val runBasePath = workflowBasePath.resolve(workflowRunDir)
        val logBasePath = runBasePath.resolve(LOGS_DIR).resolve(taskRunId.toString())

        try {
            val sbatchScript = StringBuilder(
                """
                #!/bin/bash
                #
                """.trimIndent())

            appendSbatchParam(sbatchScript, "job-name", slurmWorkflowJobName)
            appendSbatchParam(sbatchScript, "output", logBasePath.resolve("out.txt"))
            appendSbatchParam(sbatchScript, "mem", taskConfig.slurm?.mem ?: taskRunContext.memory)
            appendSbatchParam(sbatchScript, "cpus-per-task", taskConfig.slurm?.cpus ?: taskRunContext.cpus)
            appendSbatchParam(sbatchScript, "time", taskConfig.slurm?.time ?: taskRunContext.time)
            appendSbatchParam(sbatchScript, "partition", taskConfig.slurm?.partition)

            val tmpDir = Paths.get(taskRunContext.dockerDataDir, "tmp")

            sbatchScript.append("export SINGULARITY_BINDPATH=$mountDir:${taskRunContext.dockerDataDir}\n")
            sbatchScript.append("export SINGULARITYENV_TMP_DIR=$tmpDir\n")
            sbatchScript.append("mkdir $tmpDir\n")

            sbatchScript.append("\n# Copy local files needed for job into mounted singularity data directory.\n")

            // Add downloading cached input files into mounted dir to script
            for (cachedInputFile in cachedInputFiles) {
                val cachedFilePath = Paths.get(workflowBasePath.toString(), INPUTS_DIR, cachedInputFile.path)
                val mountDirFilePath = mountDir.resolve(cachedInputFile.path)
                sbatchScript.append("cp $cachedFilePath $mountDirFilePath\n")
            }

            // Add copying local non-cached input files into mounted dir to script.
            val localCopyInputFiles = downloadInputFiles.filterIsInstance<LocalInputFile>()
            for (localCopyInputFile in localCopyInputFiles) {
                val mountDirFilePath = mountDir.resolve(localCopyInputFile.path)
                sbatchScript.append("cp ${localCopyInputFile.localPath} $mountDirFilePath\n")
            }

            sbatchScript.append("\n# Download files to mounted directory using singularity.\n")
            val remoteDownloadInputFiles = downloadInputFiles.filter { it !is LocalInputFile }
            for (remoteDownloadInputFile in remoteDownloadInputFiles) {
                val downloadCommand = remoteDownloadInputFile.downloadFileCommand(taskRunContext.dockerDataDir)
                sbatchScript.append("singularity exec docker://${remoteDownloadInputFile.downloadFileImage()} $downloadCommand\n")
            }

            // Add running the task to script
            sbatchScript.append("\n# Run task command.\n")
            sbatchScript.append("singularity exec docker://${taskRunContext.dockerImage} <<END_CMD\n${taskRunContext.command}\nEND_CMD\n")

            // Add copying output files into output dir to script
            sbatchScript.append("\n# Copy output files out of mounted directory.\n")
            for (outputFile in outputFilesOut) {
                val cachedFilePath = Paths.get(workflowBasePath.toString(), OUTPUTS_DIR, outputFile.path)
                val mountDirFilePath = mountDir.resolve(outputFile.path)
                sbatchScript.append("cp $mountDirFilePath $cachedFilePath\n")
            }

            val sbatchCommand =
                """
                sbatch <<END_BATCH
                $sbatchScript
                END_BATCH
                """.trimIndent()
            commandExecutor.exec(sbatchCommand)
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