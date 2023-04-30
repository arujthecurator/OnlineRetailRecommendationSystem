package consumer

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import scala.collection.JavaConverters._


case class AggregatedData(
                 InvoiceNo: String,
                 TotalAmount: Double,
                 CustomerID: String,
                 Country: String,
                 StockCode: List[String]
               )

object DiscountConsumer extends App {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList("orders-topic"))

  var lastInvoiceId: String = ""
  var stokeCodeRecords: List[String] = List.empty[String]
  var totalAmount: Double = 0

  while (true) {
    val records = consumer.poll(Duration.ofMillis(1000))
    for (record <- records.asScala) {
      val invoice = record.value().split(",")(0).split(":")(1).replaceAll("\"", "")
      val stockCode = record.value().split(",")(1).split(":")(1).replaceAll("\"", "")
      val country = record.value().split(",")(7).split(":")(1).replaceAll("\"", "").replaceAll("}", "")
      val CustomerID = record.value().split(",")(6).split(":")(1).replaceAll("\"", "")
      val quantity:Integer = record.value().split(",")(3).split(":")(1).replaceAll("\"", "").toInt
      val amount:Double = record.value().split(",")(5).split(":")(1).replaceAll("\"", "").toDouble
      totalAmount = totalAmount + (quantity* amount)
      if (invoice != lastInvoiceId && stokeCodeRecords.nonEmpty) {
        if(totalAmount > 100){
          totalAmount = totalAmount - (totalAmount/10)  //discount
        }
        val data = AggregatedData(
          lastInvoiceId,
          totalAmount,
          CustomerID,
          country,
          stokeCodeRecords
        )
        implicit val formats: Formats = DefaultFormats
        val jsonData = Serialization.write(data)

        println(jsonData)
        stokeCodeRecords = List.empty[String]
        totalAmount = 0
      }

      stokeCodeRecords = stockCode :: stokeCodeRecords
      lastInvoiceId = invoice
    }

  }

  consumer.close()
}
