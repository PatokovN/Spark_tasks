ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
lazy val root = (project in file(".")).settings(
  name := "Searching_data"
)

val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"
)
