package consumer

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._


case class AggregatedData(
                           InvoiceNo: String,
                           TotalAmount: Double,
                           CustomerID: String,
                           Country: String,
                           StockCode: List[String]
                         )

object DiscountConsumer extends App {

  // Set up Kafka consumer properties
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  // Create a Kafka consumer instance
  val consumer = new KafkaConsumer[String, String](props)

  // Subscribe to the orders-topic topic
  consumer.subscribe(Collections.singletonList("orders-topic"))

  // Initialize variables to hold state across records
  var lastInvoiceId: String = ""
  var stockCodeRecords: List[String] = List.empty[String]
  var totalAmount: Double = 0

  // Poll for records from the Kafka topic and process them
  while (true) {
    val records = consumer.poll(Duration.ofMillis(1000))

    for (record <- records.asScala) {
      // Extract data from the record
      val invoice = getValueByKey(record.value(), "invoiceNo")
      val stockCode = getValueByKey(record.value(), "stockCode")
      val country = getValueByKey(record.value(), "country")
      val customerID = getValueByKey(record.value(), "customerID")
      val quantity = getValueByKey(record.value(), "quantity").toInt
      val amount = getValueByKey(record.value(), "unitPrice").toDouble

      // Aggregate data based on the invoice number
      totalAmount = totalAmount + (quantity * amount)
      if (invoice != lastInvoiceId && stockCodeRecords.nonEmpty) {
        // Apply discount for orders with total amount over $100
        if(totalAmount > 100){
          totalAmount = 0.9* totalAmount //discount
        }
        // Create aggregated data object and serialize it to JSON
        val data = AggregatedData(
          lastInvoiceId,
          totalAmount,
          customerID,
          country,
          stockCodeRecords
        )
        implicit val formats: Formats = DefaultFormats
        val jsonData = Serialization.write(data)

        // Print the JSON data to the console - producer comes here
        println(jsonData)

        // Reset state for the next invoice
        stockCodeRecords = List.empty[String]
        totalAmount = 0
      }

      // Update state with current record data
      stockCodeRecords = stockCode :: stockCodeRecords
      lastInvoiceId = invoice
    }
  }

  // Close the Kafka consumer when done
  consumer.close()

  // Helper function to extract the value of a field from a JSON string
  private def getValueByKey(jsonString: String, key: String): String = {
    implicit val formats: Formats = DefaultFormats

    val json = parse(jsonString)
    (json \ key).extract[String]
  }
}
