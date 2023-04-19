name := "kafka-producer-consumer"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.1",
  "org.json4s" %% "json4s-native" % "4.0.3",
  "org.slf4j" % "slf4j-nop" % "1.7.32"
)