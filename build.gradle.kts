import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.10"
    id("maven-publish")
    id("com.jfrog.bintray") version "1.8.1"
}

group = "io.krews"
version = "0.2.4"

repositories {
    maven { setUrl("http://dl.bintray.com/kotlin/kotlin-eap") }
    mavenCentral()
    jcenter()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile(kotlin("reflect"))
    compile("io.github.microutils","kotlin-logging","1.6.10")
    compile("ch.qos.logback", "logback-classic","1.2.3")
    compile("io.projectreactor", "reactor-core", "3.2.0.RELEASE")
    compile("com.github.docker-java", "docker-java", "3.0.14")
    // This one is needed to fix a warning from docker-java
    compile("javax.activation", "activation", "1.1.1")
    compile( "org.apache.commons", "commons-lang3", "3.8.1")
    compile("com.typesafe", "config", "1.2.1")
    compile("com.fasterxml.jackson.module", "jackson-module-kotlin", "2.9.7")
    compile("org.jetbrains.exposed", "exposed", "0.10.5")
    compile("org.xerial", "sqlite-jdbc", "3.25.2")
    compile("org.flywaydb", "flyway-core", "5.2.0")
    compile("com.zaxxer", "HikariCP", "3.1.0")
    compile("com.google.apis", "google-api-services-genomics", "v2alpha1-rev20181017-1.26.0")
    compile("com.google.apis", "google-api-services-storage", "v1-rev20180905-1.26.0")
    compile("com.github.ajalt","clikt", "1.5.0")
    testCompile("io.kotlintest", "kotlintest-runner-junit5", "3.1.10")
    testImplementation("io.mockk", "mockk", "1.8.13")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
    systemProperties = System.getProperties().map { it.key.toString() to it.value }.toMap()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

val sourcesJar by tasks.creating(Jar::class) {
    classifier = "sources"
    from(java.sourceSets["main"].allSource)
}


val publicationName = "krews"
publishing {
    publications.invoke {
        publicationName(MavenPublication::class) {
            from(components["java"])
            artifact(sourcesJar)
        }
    }
}

bintray {
    user = if(hasProperty("bintrayUser")) property("bintrayUser") as String? else System.getenv("BINTRAY_USER")
    key = if(hasProperty("bintrayKey")) property("bintrayKey") as String? else System.getenv("BINTRAY_KEY")
    setPublications(publicationName)
    publish = true
    override = true
    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "maven"
        name = "krews"
        githubRepo = "weng-lab/krews"
        vcsUrl = "https://github.com/weng-lab/krews"
        setLabels("kotlin")
        setLicenses("MIT")
    })
}