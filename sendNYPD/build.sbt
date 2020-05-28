name := "sendNYPD"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"


