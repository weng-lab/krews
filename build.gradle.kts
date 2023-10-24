import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.kotlin.dsl.registering
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.get

plugins {
    kotlin("jvm") version "1.7.0"
    id("maven-publish")
    id("com.jfrog.bintray") version "1.8.1"
}

group = "io.krews"
version = "0.15.2"

repositories {
    maven { setUrl("https://dl.bintray.com/kotlin/kotlin-eap") }
    mavenCentral()
    jcenter()
    mavenLocal()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))
    implementation("io.github.microutils","kotlin-logging","1.6.10")
    implementation("ch.qos.logback", "logback-classic","1.2.3")
    implementation("io.projectreactor", "reactor-core", "3.2.6.RELEASE")
    implementation("com.github.docker-java", "docker-java", "3.3.0")
    // This one is needed to fix a warning from docker-java
    implementation("javax.activation", "activation", "1.1.1")
    implementation( "org.apache.commons", "commons-lang3", "3.8.1")
    implementation("com.typesafe", "config", "1.2.1")
    implementation("com.fasterxml.jackson.module", "jackson-module-kotlin", "2.9.7")
    implementation("org.jetbrains.exposed", "exposed", "0.10.5")
    implementation("org.xerial", "sqlite-jdbc", "3.25.2")
    implementation("org.flywaydb", "flyway-core", "5.2.0")
    implementation("com.zaxxer", "HikariCP", "3.1.0")
    implementation("com.google.apis", "google-api-services-lifesciences", "v2beta-rev20200220-1.30.9")
    implementation("com.google.apis", "google-api-services-storage", "v1-rev20200326-1.30.9")
    implementation("com.google.auth", "google-auth-library-oauth2-http", "0.20.0")
    implementation("com.github.ajalt","clikt", "1.5.0")
    implementation( "org.jetbrains.kotlinx", "kotlinx-html-jvm", "0.6.11")
    implementation( "org.jetbrains.kotlinx", "kotlinx-coroutines-core", "1.1.1")
    testImplementation("org.junit.jupiter", "junit-jupiter", "5.4.0")
    testImplementation("org.assertj", "assertj-core", "3.11.1")
    testImplementation("io.mockk", "mockk", "1.9.3")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    from(sourceSets["main"].allSource)
}

val publicationName = "krews"
publishing {
    publications {
        create<MavenPublication>(publicationName) {
            from(components["java"])
            artifact(sourcesJar.get())
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/weng-lab/krews")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
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
