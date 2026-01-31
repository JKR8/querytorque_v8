plugins {
    java
    application
}

group = "com.qtcalcite"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    // CLI framework
    implementation("info.picocli:picocli:4.7.7")
    annotationProcessor("info.picocli:picocli-codegen:4.7.7")

    // Apache Calcite for query optimization
    implementation("org.apache.calcite:calcite-core:1.37.0")
    implementation("org.apache.calcite:calcite-babel:1.37.0")

    // DuckDB JDBC driver
    implementation("org.duckdb:duckdb_jdbc:1.1.0")

    // Configuration
    implementation("org.yaml:snakeyaml:2.2")

    // JSON parsing
    implementation("com.google.code.gson:gson:2.10.1")

    // HTTP client for DeepSeek API
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.12")
    implementation("org.slf4j:slf4j-simple:2.0.12")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.qtcalcite.QTCalcite")
}

tasks.test {
    useJUnitPlatform()
    // Pass system properties to tests
    systemProperties(System.getProperties().toMap() as Map<String, Any>)
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.qtcalcite.QTCalcite"
    }
}

// Create fat jar with all dependencies
tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Main-Class"] = "com.qtcalcite.QTCalcite"
    }
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    }) {
        // Exclude signature files from signed JARs to avoid SecurityException
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")
    }
}
