package krews

import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.matchers.file.shouldExist
import io.kotlintest.specs.StringSpec
import java.nio.file.Files
import java.nio.file.Paths


class AppTests : StringSpec() {
    override fun tags() = setOf(Integration)

    private val testDir = Paths.get("app-test").toAbsolutePath()!!
    private val inputsDir = testDir.resolve("inputs")
    private val outputsDir = testDir.resolve("outputs")
    private val base64Path = outputsDir.resolve("base64")
    private val gzipPath = outputsDir.resolve("gzip")
    private val sampleFilesDir = testDir.resolve("sample-files-dir")!!
    private val configFile = testDir.resolve("app-test.conf")!!
    private val config =
        """
        local-files-base-dir = $testDir
        params {
            sample-files-dir = $sampleFilesDir
        }
        task.base64.params = {
            some-val = test
        }
        """.trimIndent()

    override fun beforeSpec(description: Description, spec: Spec) {
        // Create temp sample files dir (and parent test dir) to use for this set of tests
        Files.createDirectories(sampleFilesDir)
        Files.createFile(configFile)
        Files.write(configFile, config.toByteArray())
        val inputFile = Files.createFile(sampleFilesDir.resolve("test.txt"))
        Files.write(inputFile, "I am a test file".toByteArray())
    }

    override fun afterSpec(description: Description, spec: Spec) {
        // Clean up temporary dirs
        Files.walk(testDir)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }

    init {
        "App run() should execute a simple workflow locally" {
            run(localFilesWorkflow(), arrayOf("-o", "local", "-c", "$configFile"))

            val dbPath = testDir.resolve(Paths.get("state", "metadata.db"))
            dbPath.shouldExist()

            inputsDir.resolve("test.txt").shouldExist()
            base64Path.resolve("test.b64").shouldExist()
            gzipPath.resolve("test.b64.gz").shouldExist()
        }
    }
}