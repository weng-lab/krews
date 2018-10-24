package krews

import reactor.core.publisher.toFlux

object SimpleWorkflow : Workflow("config-sample") {
    val messages = IntArray(10) { it }.map { it to "I am message #$it" }.toFlux()

    val base64 = task<Pair<Int, String>, File>("sample") {
        image("alpine:3.8")
        input { messages }
        outputFn { File("base64/${inputItem.first}.txt") }
        scriptFn {
            """
            mkdir base64
            echo "${inputItem.second}" | base64 > base64/${inputItem.first}.txt
            """.trimIndent()
        }
    }

    val gzip = task<File, File>("sample2") {
        image("alpine:3.8")
        input { base64.output }
        outputFn { File("${inputItem.path}.gz") }
        scriptFn { "gzip ${inputItem.path}" }
    }
}