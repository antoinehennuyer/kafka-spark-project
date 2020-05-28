name := "sendNYPD"

version := "0.1"


scalaVersion := "2.12.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"



