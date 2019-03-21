# Quick Start

The fastest way to get started is to clone the [krews-boilerplate](https://github.com/weng-lab/krews-boilerplate)

## Creating a Project
Instructions on setting up Kotlin projects can be found [here](https://kotlinlang.org/docs/tutorials/getting-started.html).

## Installing Krews Dependency

You can get install the Krews library using any Maven compatible build system

### Gradle
```kotlin
dependencies {
    compile("io.krews", "krews", "0.5.18")
}
```

### Maven
```xml
<dependency>
  <groupId>io.krews</groupId>
  <artifactId>krews</artifactId>
  <version>0.5.18</version>
</dependency>
```

## Building

It's highly recommended that you build the application into an executable Jar.

On Gradle, this can be done using the [Shadow Plugin](https://imperceptiblethoughts.com/shadow/)

On Maven, this can be done using the [Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/examples/executable-jar.html)

If you'd like to use your project as a library to be referenced in other projects / workflows, 
you'll need to also create a sources Jar.

Publishing your Jars to a Maven Repository like [Bintray](https://bintray.com) will give you a free (for open source) 
place to store your libraries and executables.
