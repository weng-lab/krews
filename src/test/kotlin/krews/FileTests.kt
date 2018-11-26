package krews

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import krews.file.parseGSURL

class FileTests : StringSpec({
    "parseGSURL correctly parses elements of GS URL" {
        val x = parseGSURL("gs://abc/x/y/z.txt")
        x.bucket shouldBe "abc"
        x.objectPath shouldBe "x/y/z.txt"
        x.fileName shouldBe "z.txt"
    }
})
