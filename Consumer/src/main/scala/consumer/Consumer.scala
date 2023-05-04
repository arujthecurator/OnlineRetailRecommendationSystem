package consumer

import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import scala.collection.JavaConverters._
import scala.collection.mutable

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

object Consumer extends App {
  // Configuration for Kafka consumer
  val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-consumer-group")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  // Create a Kafka consumer
  val consumer = new KafkaConsumer[String, String](consumerProps)

  // Subscribe to the "orders-topic"
  consumer.subscribe(Collections.singletonList("orders-topic"))

  // Configuration for Kafka producer
  val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  // Create a Kafka producer
  val producer = new KafkaProducer[String, String](producerProps)

  // Process each record received by the consumer
  while (true) {
    val records = consumer.poll(1000)
    for (record <- records.asScala) {
      val data = extractData(record.value()) // Extract data from the record value
      val continent = getContinent(data.Country) // Get the continent based on the country
      val topic = s"$continent-topic" // Create topic based on continent
      val message = new ProducerRecord[String, String](topic, data.InvoiceNo, record.value())
      producer.send(message)
      println(message)
    }
  }

  // Close the Kafka producer and consumer
  producer.close()
  consumer.close()

  // Helper function to extract data from the record value
  def extractData(value: String): Data = {
    val fields = value.split(",")
    Data(
      fields(0).split(":")(1).replaceAll("\"", ""),
      fields(1).split(":")(1).replaceAll("\"", ""),
      fields(2).split(":")(1).replaceAll("\"", ""),
      fields(3).split(":")(1).toInt,
      fields(4).split(":")(1).replaceAll("\"", ""),
      fields(5).split(":")(1).toDouble,
      fields(6).split(":")(1).replaceAll("\"", ""),
      fields(7).split(":")(1).replaceAll("\"", "").replaceAll("}", "")
    )
  }

  // Helper function to get the continent based on the country
  def getContinent(country: String): String = {
    country match {
      case "Singapore" | "Hong Kong" | "Saudi Arabia" | "Japan" | "United Arab Emirates" | "Lebanon" | "Bahrain" | "Cyprus" | "Israel" | "Turkey" | "Russia" | "Kuwait" | "Qatar" | "Jordan" | "Oman" | "India" | "China" => "asia"
      case "Portugal" | "Iceland" | "Malta" | "Greece" | "Netherlands" | "Sweden" | "Austria" | "Poland" | "France" | "Lithuania" | "RSA" | "Channel Islands" | "European Community" | "United Kingdom" | "Switzerland" | "Spain" | "Czech Republic" | "Belgium" | "Norway" | "EIRE" | "Finland" | "Denmark" | "Italy" | "Germany" => "europe"
      case "Brazil" | "Argentina" => "south-america"
      case "USA" | "Canada" => "north-america"
      case "Australia" | "New Zealand" => "australia"
      case _ => "unspecified"
    }
  }
}
