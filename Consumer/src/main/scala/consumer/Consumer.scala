package consumer

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConverters.asScalaIteratorConverter

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

  while (true) {
    val records = consumer.poll(Duration.ofSeconds(1))
    for (record <- records.iterator().asScala.filter(!_.value.isEmpty)) {
      println(record)
    }
  }

  consumer.close()
}

