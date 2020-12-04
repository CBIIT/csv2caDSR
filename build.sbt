// Publication information
name         := "csv2cadsr"
organization := "org.renci.ccdh"
version      := "0.1-SNAPSHOT"

// Code license
licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))

// Scalac options.
scalaVersion := "2.13.1"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-Ywarn-unused")

addCompilerPlugin(scalafixSemanticdb)
scalacOptions in Test ++= Seq("-Yrangepos")

// Set up the main class.
mainClass in (Compile, run) := Some("org.renci.ccdh.csv2cadsr.csv2caDSR")

// Fork when running.
fork in run := true

// Set up testing.
testFrameworks += new TestFramework("utest.runner.Framework")

// Code formatting and linting tools.
wartremoverWarnings ++= Warts.unsafe

addCommandAlias(
  "scalafixCheckAll",
  "; compile:scalafix --check ; test:scalafix --check"
)

// Library dependencies.
libraryDependencies ++= {
  Seq(
    // Command line argument parsing
    "com.github.alexarchambault" %% "case-app"                % "2.0.4",

    // CSV input and output
    "com.github.tototoshi"        %% "scala-csv"              % "1.3.6",

    // JSON reading/writing
    "org.json4s"                  %% "json4s-native"          % "3.6.9",

    // XML reading/writing
    "org.scala-lang.modules"      %% "scala-xml"              % "1.3.0",

    // Logging
    "com.typesafe.scala-logging"  %% "scala-logging"          % "3.9.2",
    "com.outr"                    %% "scribe"                 % "2.7.12"
  )
}
