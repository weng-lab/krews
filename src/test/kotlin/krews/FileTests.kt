package krews

import krews.file.parseGSURL
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*

class GSFileUtilTests {
    @Test
    fun `parseGSURL correctly parses elements of GS URL`() {
        val x = parseGSURL("gs://abc/x/y/z.txt")
        assertThat(x.bucket).isEqualTo("abc")
        assertThat(x.objectPath).isEqualTo("x/y/z.txt")
        assertThat(x.fileName).isEqualTo("z.txt")
    }
}
