package krews

import krews.util.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.nio.file.*

class AppTests {

    private val testDir = Paths.get("app-test").toAbsolutePath()!!
    private val outputsDir = testDir.resolve("outputs")
    private val base64Path = outputsDir.resolve("base64")
    private val gzipPath = outputsDir.resolve("gzip")
    private val sampleFilesDir = testDir.resolve("sample-files-dir")!!
    private val configFile = testDir.resolve("app-test.conf")!!
    private val config =
        """
        working-dir = "$testDir"

        params {
            sample-files-dir = "$sampleFilesDir"
        }
        task.base64.params = {
            some-val = test
        }
        """.trimIndent()

    @BeforeAll
    fun beforeTest() {
        deleteDir(testDir)
        // Create temp sample files dir (and parent test dir) to use for this set of tests
        createFile(configFile, config)
        val inputFile = sampleFilesDir.resolve("test.txt")
        createFile(inputFile, "I am a test file")
    }

    @AfterAll
    fun afterTests() = deleteDir(testDir)

    @Test
    fun `App run() should execute a simple workflow locally`() {
        run(localFilesWorkflow(), arrayOf("-o", "local", "-c", "$configFile"))

        assertThat(sampleFilesDir.resolve("test.txt")).exists()
        assertThat(base64Path.resolve("test.b64")).exists()
        assertThat(gzipPath.resolve("test.b64.gz")).exists()
    }
}