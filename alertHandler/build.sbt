name := "Consumer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.8"

mainClass := Some("alertHandler")
