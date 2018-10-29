package krews

import reactor.core.publisher.toFlux

object SimpleWorkflow : Workflow("config-sample") {
    val messages = IntArray(5) { it }.map { it to "I am message #$it" }.toFlux()

    val base64 = krews.task<Pair<Int, String>, WFile>("base64") {
        docker {
            image = "alpine:3.8"
            dataDir = "/data"
        }

        input = krews.messages
        outputFn { WFile("base64/${inputItem.first}.txt") }
        scriptFn {
            """
            mkdir /data/base64
            echo "${inputItem.second}" | base64 > /data/base64/${inputItem.first}.txt
            """.trimIndent()
        }
    }

    val gzip = krews.task<WFile, WFile>("gzip") {
        docker {
            image = "alpine:3.8"
            dataDir = "/data"
        }

        input = base64.output
        outputFn { WFile("gzip/${inputItem.filename()}.gz") }
        scriptFn {
            """
            mkdir gzip
            gzip /data/${inputItem.path} > /data/gzip/${inputItem.filename()}.gz
            """.trimIndent()
        }
    }
}