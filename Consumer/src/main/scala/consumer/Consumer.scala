package consumer

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConverters.asScalaIteratorConverter
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
  //def main(args: Array[String]): Unit = {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(Collections.singletonList("orders-topic"))

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](producerProps)

  val descriptionsByInvoice: mutable.Map[String, mutable.ListBuffer[String]] = mutable.Map()

  while (true) {
    val records = consumer.poll(1000)
    for (record <- records.asScala) {
      val invoice = record.value().split(",")(0).split(":")(1).replaceAll("\"", "")
      val description = record.value().split(",")(2).split(":")(1).replaceAll("\"", "")
      if (!descriptionsByInvoice.contains(invoice)) {
        descriptionsByInvoice.put(invoice, mutable.ListBuffer(description))
      } else {
        descriptionsByInvoice(invoice) += description
      }

      val country = record.value().split(",")(7).split(":")(1).replaceAll("\"", "").replaceAll("}", "")
      val continent = country match {
        case "Singapore" | "Hong Kong" | "Saudi Arabia" | "Japan" | "United Arab Emirates" | "Lebanon" | "Bahrain" | "Cyprus" | "Israel" | "Turkey" | "Russia" | "Kuwait" | "Qatar" | "Jordan" | "Oman" | "India" | "China" => "asia"
        case "Portugal" | "Iceland" | "Malta" | "Greece" | "Netherlands" | "Sweden" | "Austria" | "Poland" | "France" | "Lithuania" | "RSA" | "Channel Islands" | "European Community" | "United Kingdom" | "Switzerland" | "Spain" | "Czech Republic" | "Belgium" | "Norway" | "EIRE" | "Finland" | "Denmark" | "Italy" | "Germany" => "europe"
        case "Brazil" | "Argentina" => "south-america"
        case "USA" | "Canada" => "north-america"
        case "Australia" | "New Zealand" => "australia"
        case _ => "unspecified"
      }
      val topic = continent+"-topic"
      val message = new ProducerRecord[String, String](topic, invoice, record.value())
      producer.send(message)
    }

    if (descriptionsByInvoice.nonEmpty) {
      println("Descriptions by Invoice:")
      for ((invoice, descriptions) <- descriptionsByInvoice) {
        println(s"Invoice $invoice:")
        descriptions.foreach(description => println(s"\t$description"))
      }
    }

  }

  producer.close()
  consumer.close()
}

