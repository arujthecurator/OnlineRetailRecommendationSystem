package producer

import java.util.Properties
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.native.Serialization

case class Data(
                 InvoiceNo: String,
                 StockCode: String,
                 Description: String,
                 Quantity: Int,
                 InvoiceDate: String,
                 UnitPrice: Double,
                 CustomerID: String,
                 Country: String
               )

object Producer extends App {
  //def main(args: Array[String]): Unit = {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val filename = "src/main/scala/producer/online.retail.log"
  for (line <- Source.fromFile(filename).getLines()) {
    val parts = line.split(",")
    val data = Data(
      parts(0),
      parts(1),
      parts(2),
      parts(3).toInt,
      parts(4),
      parts(5).toDouble,
      parts(6),
      parts(7)
    )

    implicit val formats: Formats = DefaultFormats
    val jsonData = Serialization.write(data)

    val record = new ProducerRecord[String, String]("test-topic", s"key_${data.InvoiceNo}", jsonData)
    producer.send(record)
    println(s"Sent record: $record")
    Thread.sleep(2000) // Sleep for 2 seconds after each line is read
    //}
  }
  producer.close()
}
