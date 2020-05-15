name := "test_scala_kafka"

version := "0.1"

scalaVersion := "2.13.2"

mainClass := Some("main")

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.5.0"
