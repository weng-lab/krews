---
title: 'Krews: Kotlin Reactive Workflows'
---

<div class="img-container">
    <img alt="logo" src="img/krews_logo.png" />
</div>

Krews, short for "Kotlin Reactive Workflows" is a framework for creating scalable and reproducible 
scientific data pipelines using [Docker Containers](https://www.docker.com/) piped together with 
[Project Reactor](https://projectreactor.io/), a mature functional reactive programming library. 

Workflows are written using a [Kotlin](https://kotlinlang.org/) DSL in plain old Kotlin projects, 
meaning you get all the benefits of a modern, type-safe, functional language with fantastic tooling.

# Quick Example

```kotlin
fun main(args: Array<String>) = run(sampleWorkflow, args)

val sampleWorkflow = workflow("sample-workflow") {

    // Reactive "Flux" list object for the numbers 1 to 5
    val range = (1..5).toFlux()
    
    task<Int, File>("base64", range) {
        dockerImage = "alpine:3.9"
        output = OutputFile("base64/$input.b64")
        command =
            """
            mkdir -p /data/base64
            echo "Hello World $input!" | base64 > /data/base64/$input.b64
            """
    }
}
```

*Configuration*
```hocon
working-dir = /data/sample-workflow
```

*Run command*
```
java -jar my-app.jar --on local --config path/to/my-config.conf
```

# Supported Platforms
- [Google Cloud Life Sciences](https://cloud.google.com/life-sciences/)
- [Slurm](https://slurm.schedmd.com/)
- Local Docker (Single Machine)