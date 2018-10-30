package krews

import reactor.core.publisher.toFlux

object SimpleWorkflow : Workflow("config-sample") {
    val messages = IntArray(5) { it }.map { it to "I am message #$it" }.toFlux()

    val base64 = task<Pair<Int, String>, WFile>("base64") {
        dockerImage = "alpine:3.8"

        input = messages
        outputFn { WFile("base64/${inputItem.first}.txt") }
        commandFn {
            """
            echo "executing base64 on ${inputItem.first}"
            mkdir -p /data/base64
            echo "${inputItem.second}" | base64 > /data/base64/${inputItem.first}.txt
            """.trimIndent()
        }
    }

    val gzip = task<WFile, WFile>("gzip") {
        dockerImage = "alpine:3.8"

        input = base64.output
        outputFn { WFile("gzip/${inputItem.filename()}.gz") }
        commandFn {
            """
            echo "executing gzip on ${inputItem.filename()}"
            mkdir -p /data/gzip
            gzip /data/${inputItem.path} > /data/gzip/${inputItem.filename()}.gz
            """.trimIndent()
        }
    }
}