package producer

import java.util.Properties
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.native.Serialization

// class for the order data
case class OrderData(
                      invoiceNo: String,
                      stockCode: String,
                      description: String,
                      quantity: Int,
                      invoiceDate: String,
                      unitPrice: Double,
                      customerID: String,
                      country: String
                    )

object Producer extends App {

  // Set up Kafka producer configuration properties
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // Create a new Kafka producer using the configuration properties
  val producer = new KafkaProducer[String, String](props)

  // Read the order data from a file
  val filename = "src/main/scala/producer/online.retail.log"
  val lines = Source.fromFile(filename).getLines().toList

  // Send the order data to Kafka
  sendOrdersToKafka(lines, producer)

  // Close the Kafka producer
  producer.close()

  // Helper function to parse a single line of order data and create an OrderData object
  private def parseOrderData(line: String): OrderData = {
    val parts = line.split(",")
    OrderData(
      parts(0),
      parts(1),
      parts(2),
      parts(3).toInt,
      parts(4),
      parts(5).toDouble,
      parts(6),
      parts(7)
    )
  }

  // Helper function to send a list of order data to Kafka
  private def sendOrdersToKafka(lines: List[String], producer: KafkaProducer[String, String]): Unit = {

    // Set up JSON serialization
    implicit val formats: Formats = DefaultFormats

    // Loop through each line of order data, parse it, convert it to JSON, and send it to Kafka
    for (line <- lines) {
      val orderData = parseOrderData(line)
      val jsonData = Serialization.write(orderData)

      val record = new ProducerRecord[String, String]("orders-topic", s"key_${orderData.invoiceNo}", jsonData)
      producer.send(record)
      println(jsonData)

      Thread.sleep(2000) // Sleep for 50 milliseconds after each line is read
    }
  }
}
