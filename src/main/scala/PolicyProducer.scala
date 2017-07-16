package zxtx

import scala.io.Source
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

object PolicyProducer extends App{
  if (args.length < 2) {
    System.err.println("Usage: PolicyProducer <metadataBrokerList> <topic>")
    System.exit(1)
  }

  val Array(brokers, topic) = args

  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer") // Kafka avro message stream comes in as a byte array
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

 val producer = new KafkaProducer[String, Array[Byte]](props)
  
  while(true){
    val policy: GenericRecord = new GenericData.Record(AvroUtil.policySchema)
    policy.put("id", 123456)
    policy.put("name", "众行天下")
    policy.put("email", "fourbroad@51zxtx.com")
    
    producer.send(new ProducerRecord[String, Array[Byte]](topic,null, AvroUtil.policyEncoder(policy)))

    println(System.currentTimeMillis + "Send a new policy......")
    println(policy)

    Thread.sleep(1000)
  }
}

