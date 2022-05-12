import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin ("jvm") version "1.6.10"
  application
  id("com.github.johnrengelman.shadow") version "7.0.0"
}

group = "org.nicktorwald"
version = "1.0.0-SNAPSHOT"

repositories {
  mavenCentral()
}

val vertxVersion = "4.2.7"

val mainVerticleName = "org.nicktorwald.rest.RestService"
val launcherClassName = "io.vertx.core.Launcher"

application {
  mainClass.set(launcherClassName)
}

dependencies {
  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
  implementation("io.vertx:vertx-config")
  implementation("io.vertx:vertx-web")
  implementation("io.vertx:vertx-lang-kotlin-coroutines")
  implementation("io.vertx:vertx-lang-kotlin")

  implementation("io.github.microutils:kotlin-logging-jvm:2.0.11")
  implementation("ch.qos.logback:logback-classic:1.2.6")

  implementation(kotlin("stdlib-jdk8"))

  runtimeOnly("io.netty:netty-resolver-dns-native-macos:4.1.77.Final:osx-x86_64")
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions.jvmTarget = "1.8"

tasks.withType<ShadowJar> {
  archiveClassifier.set("fat")
  manifest {
    attributes(mapOf("Main-Verticle" to mainVerticleName))
  }
  mergeServiceFiles()
}

tasks.withType<JavaExec> {
  args = listOf("run", mainVerticleName)
}
