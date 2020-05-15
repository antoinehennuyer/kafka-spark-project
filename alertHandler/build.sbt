name := "Consumer"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"

mainClass := Some("alertHandler")