package krews

import reactor.core.publisher.toMono

val sample = task<String, WFile>("Sample") {
    image("test")
    input {
        "".toMono()
    }
    outputFn {
        WFile("")
    }
    scriptFn {
        "testcmd $inputItem"
    }
}

val sample2 = task<WFile, String>("Sample2") {
    labels("small", "fast")
    image("test")
    input {
        sample.output
    }
    outputFn {
        ""
    }
    scriptFn {
        "testcmd ${inputItem.path}"
    }
}

fun main(args: List<String>) = run("my-workflow", args)