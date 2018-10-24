package krews

import reactor.core.publisher.toMono

val sample = task<String, File>("Sample") {
    image("test")
    input {
        "".toMono()
    }
    outputFn {
        File("")
    }
    scriptFn {
        "testcmd $inputItem"
    }
}

val sample2 = task<File, String>("Sample2") {
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