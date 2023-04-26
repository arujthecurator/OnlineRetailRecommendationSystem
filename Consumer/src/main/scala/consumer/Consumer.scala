package consumer

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
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

  consumer.subscribe(Collections.singletonList("test-topic"))

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
    }

    if (descriptionsByInvoice.nonEmpty) {
      println("Descriptions by Invoice:")
      for ((invoice, descriptions) <- descriptionsByInvoice) {
        println(s"Invoice $invoice:")
        descriptions.foreach(description => println(s"\t$description"))
      }
    }
  }

  consumer.close()
}

