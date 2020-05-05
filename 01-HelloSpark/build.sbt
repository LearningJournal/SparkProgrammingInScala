name := "HelloSpark"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"
//scalaVersion := "2.11.8"
autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"
//val sparkVersion = "2.3.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies

