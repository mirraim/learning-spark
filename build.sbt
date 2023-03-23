ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "learning-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided
    ),
    assembly / assemblyJarName := "assembly.jar"
  )
