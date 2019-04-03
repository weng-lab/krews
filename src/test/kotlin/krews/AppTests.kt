package krews

import krews.util.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.nio.file.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppTests {

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

    @BeforeAll fun beforeSpec() {
        deleteDir(testDir)
        // Create temp sample files dir (and parent test dir) to use for this set of tests
        createFile(configFile, config)
        val inputFile = sampleFilesDir.resolve("test.txt")
        createFile(inputFile, "I am a test file")
    }

    @AfterAll fun afterTests() = deleteDir(testDir)

    @Test fun `App run() should execute a simple workflow locally`() {
        run(localFilesWorkflow(), arrayOf("-o", "local", "-c", "$configFile"))

        val dbPath = testDir.resolve(Paths.get("state", "metadata.db"))
        assertThat(dbPath).exists()

        assertThat(inputsDir.resolve("test.txt")).exists()
        assertThat(base64Path.resolve("test.b64")).exists()
        assertThat(gzipPath.resolve("test.b64.gz")).exists()
    }
}