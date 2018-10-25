package krews

import reactor.core.publisher.toFlux

object SimpleWorkflow : Workflow("config-sample") {
    val messages = IntArray(10) { it }.map { it to "I am message #$it" }.toFlux()

    val base64 = task<Pair<Int, String>, WFile>("sample") {
        image("alpine:3.8")
        input { messages }
        outputFn { WFile("base64/${inputItem.first}.txt") }
        scriptFn {
            """
            mkdir base64
            echo "${inputItem.second}" | base64 > base64/${inputItem.first}.txt
            """.trimIndent()
        }
    }

    val gzip = task<WFile, WFile>("sample2") {
        image("alpine:3.8")
        input { base64.output }
        outputFn { WFile("gzip/${inputItem.filename()}.gz") }
        scriptFn {
            """
            mkdir gzip
            gzip ${inputItem.path} > gzip/${inputItem.filename()}.gz
            """.trimIndent() }
    }
}