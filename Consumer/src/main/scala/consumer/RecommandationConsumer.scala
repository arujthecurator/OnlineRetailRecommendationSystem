package consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable

object RecommendationConsumer extends App {

  // Set up Kafka consumer configuration
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  // Create a Kafka consumer instance
  val consumer = new KafkaConsumer[String, String](props)

  // Subscribe to the orders-topic
  consumer.subscribe(Collections.singletonList("orders-topic"))

  // Create a mutable map to store descriptions by invoice
  val descriptionsByInvoice: mutable.Map[String, mutable.ListBuffer[String]] = mutable.Map()

  // Run the GSP algorithm before capturing streams
  val dataPath = "src/main/scala/consumer/online.retail.log"

  /*Note: Uncomment the following code to train the model
  * Since, the model is already trained, we have commented the line*/
  //ml.gsp(dataPath, 200)

  // Continuously poll for messages
  while (true) {
    // Poll for messages every 10 seconds
    val records = consumer.poll(10000)
    for (record <- records.asScala) {
      // Extract invoice and description from the record
      val invoice = record.value().split(",")(0).split(":")(1).replaceAll("\"", "")
      val description = record.value().split(",")(2).split(":")(1).replaceAll("\"", "")
      // Add description to the map
      if (!descriptionsByInvoice.contains(invoice)) {
        descriptionsByInvoice.put(invoice, mutable.ListBuffer(description))
      } else {
        descriptionsByInvoice(invoice) += description
      }
    }

    // If there are descriptions in the map, print recommendations
    if (descriptionsByInvoice.nonEmpty) {
      println("Descriptions by Invoice:")
      for ((invoice, descriptions) <- descriptionsByInvoice) {
        println(s"Invoice $invoice:")
        println(s"Cart Items: $descriptions")
        print("Recommendation for the above cart: ")
        val inputSet = descriptions.toSet[String]
        val recommendation = ML.recommendation(inputSet)
        if(!recommendation.isEmpty) {
          println(ML.recommendation(inputSet))
        }
        else{
          println("No Recommendations")
        }
      }
    }
  }

  // Close the consumer
  consumer.close()
}
