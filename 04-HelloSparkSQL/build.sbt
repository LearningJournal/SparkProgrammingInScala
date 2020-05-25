name := "04-HelloSparkSQL"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"

autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies