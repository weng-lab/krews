import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.0-rc-146"
}

group = "io.krews"
version = "1.0-SNAPSHOT"

repositories {
    maven { setUrl("http://dl.bintray.com/kotlin/kotlin-eap") }
    mavenCentral()
    jcenter()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("io.github.microutils","kotlin-logging","1.6.10")
    compile("ch.qos.logback", "logback-classic","1.2.3")
    compile("io.projectreactor", "reactor-core", "3.2.0.RELEASE")
    compile("com.github.docker-java", "docker-java", "3.0.14")
    // This one is needed to fix a warning from docker-java
    compile("javax.activation", "activation", "1.1.1")
    compile("com.typesafe", "config", "1.2.1")
    compile("com.fasterxml.jackson.module", "jackson-module-kotlin", "2.9.7")
    compile("org.jetbrains.exposed", "exposed", "0.10.5")
    compile("org.xerial", "sqlite-jdbc", "3.25.2")
    compile("org.flywaydb", "flyway-core", "5.2.0")
    compile("com.google.apis", "google-api-services-genomics", "v2alpha1-rev20181017-1.26.0")
    compile("com.google.apis", "google-api-services-storage", "v1-rev20180905-1.26.0")
    testCompile("io.kotlintest", "kotlintest-runner-junit5", "3.1.10")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}