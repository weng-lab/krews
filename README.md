<h1 align="center">
    <img alt="logo" src="docs/img/krews_logo.png" />
</h1>

Krews, short for "Kotlin Reactive Workflows" is a framework for creating scalable and reproducible 
scientific data pipelines using [Docker Containers](https://www.docker.com/) piped together with 
[Project Reactor](https://projectreactor.io/), a mature functional reactive programming library. 

Workflows are written using a [Kotlin](https://kotlinlang.org/) DSL in plain old Kotlin projects, 
meaning you get all the benefits of a modern, type-safe, functional language with fantastic tooling.

## Documentation
Full Documentation can be found [here](https://weng-lab.github.io/krews/)

## Boilerplate
The fastest way to get started is to clone the [krews-boilerplate](https://github.com/weng-lab/krews-boilerplate). 
This is not only quick, but is set up using what we consider to be best practices.

The boilerplate README.md contains instructions on what you'll need to replace, as well as instructions 
on running and building.

## Quick Example

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

## Supported Platforms
- [Google Cloud Life Sciences](https://cloud.google.com/life-sciences/)
- [Slurm](https://slurm.schedmd.com/)
- Local Docker (Single Machine)

## Installation

### Gradle
```kotlin
dependencies {
    compile("io.krews", "krews", "0.7.0")
}
```

### Maven
```xml
<dependency>
  <groupId>io.krews</groupId>
  <artifactId>krews</artifactId>
  <version>0.7.0</version>
</dependency>
```