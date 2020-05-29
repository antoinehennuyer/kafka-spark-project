name := "stream_to_hdfs"

version := "1.0"



scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.8"
