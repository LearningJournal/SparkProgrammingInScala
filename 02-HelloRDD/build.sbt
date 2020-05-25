name := "HelloRDD"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)

libraryDependencies ++= sparkDependencies
