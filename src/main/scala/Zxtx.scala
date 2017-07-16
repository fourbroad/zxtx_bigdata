package zxtx

import kafka.serializer.DefaultDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


object Zxtx extends App {

  if(args.length < 2){
    System.err.println("Usage: <topic> <numThreads>")
    System.exit(1)
  }


  val Array(topics, numThreads) = args
  val sparkConf = new SparkConf().setAppName("ZXTX").setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  ssc.checkpoint("./checkpoint")

  val kafkaConf = Map(
    "metadata.broker.list" -> "localhost:9092", // Default kafka broker list location
    "zookeeper.connect" -> "localhost:2181",    // Default zookeeper location
    "group.id" -> "zxtx-policies",
    "zookeeper.connection.timeout.ms" -> "1000"
  ) 
  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaConf, topicMap,StorageLevel.MEMORY_ONLY_SER).map(_._2)

  val policyRDD = lines.transform{rdd =>
    rdd.map{ bytes => AvroUtil.policyDecoder(bytes) } 
  }

  policyRDD.print

  lines.foreachRDD{ rdd =>
    rdd.foreachPartition { partitionOfRecords =>
      val timestamp: Long = System.currentTimeMillis
      val prefix = "policies-"
      val suffix = ".parquet"
      val fullPath = prefix + timestamp + suffix
      val path = new Path("./policies/" + fullPath)
      
      val parquetWriter = new AvroParquetWriter[GenericRecord](path, AvroUtil.policySchema)
      partitionOfRecords.foreach{ bytes =>
        parquetWriter.write(AvroUtil.policyDecoder(bytes))
      }
      parquetWriter.close()
    }
  }

 ssc.start()
  ssc.awaitTermination()
}
