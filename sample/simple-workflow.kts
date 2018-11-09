@file:CompilerOpts("-jvm-target 1.8")
@file:DependsOn("io.krews:krews:0.1.0")

import krews.*
import reactor.core.publisher.*

val messages: Flux<Int> = (1..5).toFlux()

val base64 = task<Int, WFile>("base64") {
    dockerImage = "alpine:3.8"

    input = messages
    outputFn { WFile("base64/${inputItem}.txt") }
    commandFn {
        """
        echo "executing base64 on ${inputItem}"
        mkdir -p /data/base64
        echo "I am number ${inputItem}" | base64 > /data/base64/${inputItem}.txt
        """.trimIndent()
    }
}

task<WFile, WFile>("gzip") {
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

run("simple-workflow", args)