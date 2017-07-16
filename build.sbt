name := "zxtx.policies"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.7.7",
      "org.apache.parquet" % "parquet-avro" % "1.9.0",
      "org.apache.kafka" % "kafka_2.11" % "0.10.0.0",
      "org.apache.spark" % "spark-core_2.11" % "2.1.1",
      "org.apache.spark" % "spark-streaming_2.11" % "2.1.1",
      "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.1"
)

