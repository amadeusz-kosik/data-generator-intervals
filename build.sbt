val DefaultScalacOptions = Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions")

val ScalaTestVersion = "3.2.20"
val SparkVersion = "4.1.1"

val outputJarFileName = "data-generator-intervals.jar"
val mainClassFullName = "data.generator.intervals.Main"

lazy val root = (project in file("."))
  .settings(
    name := "data-generator-intervals",
    version := "1.0.1"
  )


ThisBuild / scalacOptions ++= DefaultScalacOptions
ThisBuild / scalaVersion := "2.13.18"

ThisBuild / Test / parallelExecution := false

ThisBuild / assembly / assemblyJarName := outputJarFileName

ThisBuild / libraryDependencies += "org.scalatest"      %% "scalatest"            % ScalaTestVersion
ThisBuild / libraryDependencies += "org.apache.spark"   %% "spark-core"           % SparkVersion      % Provided
ThisBuild / libraryDependencies += "org.apache.spark"   %% "spark-sql"            % SparkVersion      % Provided

// sbt-docker configuration
enablePlugins(DockerPlugin)

docker / dockerfile := {
  val jar = (Compile / packageBin).value

  new Dockerfile {
    from(s"spark:${SparkVersion}-scala2.13-java21-ubuntu")

    copy(jar, s"/app/$outputJarFileName")

    volume("/mnt/data")
    volume("/mnt/events")

    expose(4040)

    entryPoint(
      "/opt/spark/bin/spark-submit",
      "--class", mainClassFullName,
      "--master", "spark://spark-master:7077",
      s"/app/$outputJarFileName"
    )
  }
}

docker / imageNames := Seq(
  // Sets the latest tag
  ImageName(
    namespace = Some(organization.value),
    repository = name.value,
    tag = Some("latest")
  ),

  // Sets a name with a tag that contains the project version
  ImageName(
    namespace = Some(organization.value),
    repository = name.value,
    tag = Some(version.value)
  )
)
