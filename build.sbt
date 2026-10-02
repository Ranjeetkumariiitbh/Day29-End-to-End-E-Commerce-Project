ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.ecommerce"

val sparkVersion = "3.5.1"

// Spark on Java 17/21 needs these module opens (Java 11 accepts them too; Java 8 is NOT supported here).
val sparkJavaOpts = Seq(
  "-Xmx2g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "day29-end-to-end-ecommerce",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
    ),
    Compile / mainClass := Some("com.ecommerce.Main"),
    run / fork := true,
    run / outputStrategy := Some(StdoutOutput),
    Test / fork := true,
    Test / parallelExecution := false,
    javaOptions ++= sparkJavaOpts
  )
