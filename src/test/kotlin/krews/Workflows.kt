package krews

import krews.core.Workflow
import krews.file.OutputFile
import reactor.core.publisher.toFlux

object SimpleWorkflow : Workflow("config-sample") {
    val messages = (1..5).toFlux()

    val base64 = task<Int, OutputFile>("base64") {
        dockerImage = "alpine:3.8"

        input = messages
        outputFn { OutputFile("base64/$inputItem.txt") }
        commandFn {
            """
            echo "executing base64 on $inputItem"
            mkdir -p /data/base64
            echo "I am number $inputItem" | base64 > /data/base64/$inputItem.txt
            """.trimIndent()
        }
    }

    val gzip = task<OutputFile, OutputFile>("gzip") {
        dockerImage = "alpine:3.8"

        input = base64.output
        outputFn { OutputFile("gzip/${inputItem.filename()}.gz") }
        commandFn {
            """
            echo "executing gzip on ${inputItem.filename()}"
            mkdir -p /data/gzip
            gzip /data/${inputItem.path} > /data/gzip/${inputItem.filename()}.gz
            """.trimIndent()
        }
    }
}