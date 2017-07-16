package zxtx

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter


object AvroUtil { 
  private var schemaString = """{
    "namespace": "kakfa-avro.test",
     "type": "record",
     "name": "user",
     "fields":[
         {
            "name": "id", "type": "int"
         },
         {
            "name": "name",  "type": "string"
         },
         {
            "name": "email", "type": ["string", "null"]
         }
     ]
  }"""
  var policySchema: Schema = new Schema.Parser().parse(schemaString)

  val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](policySchema)
  def policyDecoder(bytes: Array[Byte]): GenericRecord = {
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }

  val writer = new SpecificDatumWriter[GenericRecord](policySchema)
  def policyEncoder(policy: GenericRecord): Array[Byte] = {
     val out = new ByteArrayOutputStream()
     val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
     writer.write(policy, encoder)
     encoder.flush()
     out.close()
     out.toByteArray()
  }
}
