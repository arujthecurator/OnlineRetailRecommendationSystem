# OnlineRetailRecommendationSystem
1. Install Scala plugin in Intellij IDEA, 
2. Install java8+ in your machine,
3. Install sbt in your local machine from the following link- https://www.scala-sbt.org/download.html,
4. The dataset that we have used for this project is taken from the following repository- https://archive.ics.uci.edu/ml/datasets/online+retail#


### Set up process:
1. Execute the following command to publish the docker images to docker
  "docker compose -f ./docker-compose-expose.yml up --detach"
2. Clone the project to local

### Getting the application started:
#### Producer:
1. Import the Producer to Intellij as Scala sbt project.
2. Run the producer file, We should be able to see the messages being published from the console. This means that the messages are being pushed to orders-topic.

#### Consumer:
1. Import the Consumer project separately to Intellij as Scala sbt project.
2. Now, run the consumers. Since we gave mutliple consumer applications, we run them indivually to see the execution.

##### PartitionConsumer:
1. Run the PartitionComsumer.scala file, once the application has started we can see the message being consumed from orders-topic.
2. This consumer segregates the message load to different topics based the corresponding continent topic.
3. Now run all the 6 continent based consumer scala files.
4. We should be able to see the messages being consumed as per the countries-contient mapping.

##### DiscountConsumer:
1. This consumer takes the messages from the orders-topic, waits till all the messages for the respective invoice is consumed and aggregates them and apply some discount.
2. Run the DiscountConsumer.scala file.
2. After execution, we should be able to see single message for each invoice, with mutliple stockCode in it. This explains that the orderItems as aggregated to one single order.

##### RecommandationConsumer:
1. This consumer takes the messages continuously and based on the items added in the cart, the consumer recommends the most frequently bought items with it to the customer.
2. Run the RecommandationConsumer.scala.
3. After execution, this program calls gps method in ML.scala file, which records the most frequently bought items mapping and stores them to result.log file.
4. Once the grouping of items are saved to result.log file, the program continuously consumes the messages from orders-topic and based on the items consumed, it recommends the items to customer. We should be able to see these results in the console log.
