package krews.core

import kotlinx.coroutines.*
import krews.config.*
import krews.executor.*
import krews.misc.createReport
import mu.KotlinLogging
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.nio.file.attribute.*
import java.util.*
import java.util.concurrent.*

private val log = KotlinLogging.logger {}

class WorkflowRunner(
    private val workflow: Workflow,
    private val workflowConfig: WorkflowConfig,
    private val executor: LocallyDirectedExecutor,
    runTimestampOverride: Long? = null
) {
    private lateinit var workflowRun: WorkflowRun
    private val reportPool = Executors.newSingleThreadScheduledExecutor()
    private val cacheUploadPool = Executors.newSingleThreadScheduledExecutor()
    private val workflowTime = runTimestampOverride ?: System.currentTimeMillis()
    private val workflowTmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "workflow-${workflow.name}-$workflowTime")


    fun run() {
        log.info { "Starting workflow run for workflow ${workflow.name} with timestamp $workflowTime" }
        val workflowRun = WorkflowRun(workflow.name, workflowTime)

        val cacheFilePath = workflowTmpDir.resolve(CACHE_FILENAME)
        executor.downloadFile(CACHE_FILENAME, cacheFilePath)
        val cachePermissions = PosixFilePermissions.fromString("rwxrwxrwx")
        Files.setPosixFilePermissions(cacheFilePath, cachePermissions)
        Files.setPosixFilePermissions(cacheFilePath.parent, cachePermissions)

        // Create an executor service for periodically uploading the db file
        val cacheUploadDelay = Math.max(workflowConfig.dbUploadDelay, 30)
        cacheUploadPool.scheduleWithFixedDelay({ uploadCache() },
            cacheUploadDelay, cacheUploadDelay, TimeUnit.SECONDS)

        // Create an executor service for periodically generating reports
        val reportGenerationDelay = Math.max(workflowConfig.reportGenerationDelay, 10)
        reportPool.scheduleWithFixedDelay({ generateReport() },
            reportGenerationDelay, reportGenerationDelay, TimeUnit.SECONDS)

        val workflowRunSuccessful = runWorkflow()

        workflowRun.completedSuccessfully = workflowRunSuccessful
        workflowRun.completedTime = System.currentTimeMillis()
        onShutdown()
    }

    private fun runWorkflow(): Boolean {
        val workflowGraph = buildGraph(workflow.tasks, workflowConfig.tasks)

        // Download InputFiles with Cache set to true.
        for (inputFile in workflowGraph.inputFiles) {
            if (!inputFile.cache) continue
            // TODO
        }

        updateQueue(workflowGraph.headNodes)
    }

    fun onShutdown() {
        // TODO this should propagate through TaskRunner and Executor too
        if (!hasShutdown.compareAndSet(false, true)) return
        log.info { "Shutting down..." }
        reportPool.shutdown()
        dbUploadPool.shutdown()
        // Stop the periodic db upload and report generation executor service and generate one final report.
        reportPool.awaitTermination(1000, TimeUnit.SECONDS)
        dbUploadPool.awaitTermination(1000, TimeUnit.SECONDS)

        uploadCache()
        generateReport()

        Files.walk(workflowTmpDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
        log.info { "Shutdown complete!" }
    }

    /**
     * Backs up the cache and uploads it to working directory
     */
    private fun uploadCache() {
        log.info { "Creating cache backup..." }
        val cacheBackupPath = workflowTmpDir.resolve(CACHE_FILENAME)
        log.info { "Uploading cache backup file..." }
        executor.uploadFile(cacheBackupPath, CACHE_FILENAME, backup = true)
        log.info { "Database upload complete!" }
    }

    private fun generateReport() {
        log.info { "Generating report..." }
        val workingReportPath = workflowTmpDir.resolve(REPORT_FILENAME)
        val uploadReportFile = "$RUN_DIR/${workflowRun.startTime}/$REPORT_FILENAME"

        Files.createDirectories(workingReportPath.parent)
        val writer = Files.newBufferedWriter(workingReportPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        createReport(db, workflowRun, writer)
        writer.flush()
        log.info { "Report generation complete (see: $workingReportPath)! Uploading..." }

        executor.uploadFile(workingReportPath, uploadReportFile)
        log.info { "Report upload complete!" }
    }
}
