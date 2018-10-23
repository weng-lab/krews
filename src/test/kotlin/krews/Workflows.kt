package krews

import reactor.core.publisher.toFlux

object SimpleWorkflow : Workflow("config-sample") {
    val messages = IntArray(100) { it }.map { it to "I am message #$it" }.toFlux()

    val base64 = task<Pair<Int, String>, File>("sample") {
        image("alpine:3.8")
        input { messages }
        outputFn { File("${inputItem.first}.txt") }
        scriptFn { "echo ${inputItem.second} | base64 > ${inputItem.first}.txt" }
    }

    val gzip = task<File, File>("sample2") {
        image("alpine:3.8")
        input { base64.output }
        outputFn { File("${inputItem.path}.gz") }
        scriptFn { "gzip ${inputItem.path}" }
    }
}