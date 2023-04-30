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

//case class Data(
//                 InvoiceNo: String,
//                 StockCode: String,
//                 Description: String,
//                 Quantity: Int,
//                 InvoiceDate: String,
//                 UnitPrice: Double,
//                 CustomerID: String,
//                 Country: String
//               )

object ContinentConsumerNorthAmerica extends App {
  //def main(args: Array[String]): Unit = {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(Collections.singletonList("north-america-topic"))


  while (true) {
    val records = consumer.poll(1000)
    for (record <- records.asScala) {
      val invoice = record.value().split(",")(0).split(":")(1).replaceAll("\"", "")
      val description = record.value().split(",")(2).split(":")(1).replaceAll("\"", "")
      val country = record.value().split(",")(7).split(":")(1).replaceAll("\"", "").replaceAll("}", "")
      println("Invoice: "+invoice+" Country: "+country)
    }


  }

  consumer.close()
}

