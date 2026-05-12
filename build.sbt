val DefaultScalacOptions = Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions")

val ScalaTestVersion = "3.2.20"
val SparkVersion = "4.1.1"

//// Deduplication (assemblyMergeStrategy) for sbt-assembly
//val SparkJobAssemblyMergeStrategy: String => sbtassembly.MergeStrategy = {
//  // Do not erase log4j files
//  case "plugin.properties" | "log4j.properties" =>
//    MergeStrategy.concat
//
//  // Otherwise it will fail with "Failed to find the data source: parquet."
//  case PathList("META-INF", "services",  _*) =>
//    MergeStrategy.concat
//
//  case PathList("META-INF", xs @ _*) =>
//    MergeStrategy.discard
//
//  case x =>
//    MergeStrategy.first
//}


lazy val root = (project in file("."))
  .settings(name := "data-generator-intervals")


ThisBuild / scalacOptions ++= DefaultScalacOptions
ThisBuild / scalaVersion := "2.13.18"

ThisBuild / Test / parallelExecution := false

ThisBuild / assembly / assemblyJarName := "data-generator-intervals.jar"
ThisBuild / assembly / mainClass := Some("data.generator.intervals.Main")
//ThisBuild / assembly / assemblyMergeStrategy := SparkJobAssemblyMergeStrategy


ThisBuild / libraryDependencies += "org.scalatest"      %% "scalatest"            % ScalaTestVersion
ThisBuild / libraryDependencies += "org.apache.spark"   %% "spark-core"           % SparkVersion      % Provided
ThisBuild / libraryDependencies += "org.apache.spark"   %% "spark-sql"            % SparkVersion      % Provided

