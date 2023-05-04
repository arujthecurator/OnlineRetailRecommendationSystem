package consumer

import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._

object ContinentConsumerAsia extends App {

  // Set properties for the consumer
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  // Create a KafkaConsumer instance
  val consumer = new KafkaConsumer[String, String](props)

  // Subscribe to the specified topic
  consumer.subscribe(Collections.singletonList("asia-topic"))

  // Start an infinite loop to poll records from Kafka
  while (true) {
    val records = consumer.poll(1000) // Poll records from Kafka
    for (record <- records.asScala) { // Iterate over records
      val data = extractData(record.value()) // Extract data from record value
      val invoice = data.InvoiceNo
      val country = data.Country
      println(s"Invoice: $invoice Country: $country") // Print extracted data
    }
  }

  consumer.close() // Close the KafkaConsumer instance after processing all records

  // Function to extract data from the record value
  private def extractData(recordValue: String): Data = {
    val parts = recordValue.split(",").map(_.split(":")(1).replaceAll("\"", ""))
    Data(parts(0), parts(1), parts(2), parts(3).toInt, parts(4), parts(5).toDouble, parts(6), parts(7).replaceAll("}", ""))
  }

  // Class to hold the extracted data
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

}
