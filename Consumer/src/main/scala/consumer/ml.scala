package consumer

import scala.language.postfixOps


object ml extends App {

  import scala.collection.mutable
  import scala.io.Source
  import scala.language.postfixOps

  def gsp(logFile: String, minSup: Int): List[Set[String]] = {

    // read log file and group transactions by invoice number
    val transactions = Source.fromFile(logFile).getLines().toList.map(_.split(","))
    val groupedTransactions = transactions.groupBy(_.head).values.toList.map(_.map(_ (2)).sorted)

    // initialize item frequency
    val itemFreq = mutable.Map[String, Int]().withDefaultValue(0)

    // get 1-itemsets
    for (transaction <- groupedTransactions) {
      for (item <- transaction) {
        itemFreq(item) += 1
      }
    }

    // filter itemsets that meet the minimum support threshold
    val freqItems = itemFreq.filter(_._2 >= minSup).keys.toSet

    // initialize candidate itemsets
    var candidateItemsets = freqItems.map(Set(_)).toList

    // initialize frequent itemsets
    var freqItemsets = candidateItemsets.filter(itemset => itemset.forall(freqItems.contains))

    // loop until there are no more candidate itemsets
    while (candidateItemsets.nonEmpty) {

      // generate new candidate itemsets
      val newCandidateItemsets = candidateItemsets.flatMap(itemset => freqItems.filter(!itemset.contains(_)).map(item => itemset + item))

      // count support of candidate itemsets
      val candidateSupport = mutable.Map[Set[String], Int]().withDefaultValue(0)
      for (transaction <- groupedTransactions) {
        for (candidateItemset <- newCandidateItemsets) {
          if (candidateItemset.subsetOf(transaction.toSet)) {
            candidateSupport(candidateItemset) += 1
          }
        }
      }

      // filter candidate itemsets that meet the minimum support threshold
      candidateItemsets = candidateSupport.filter(_._2 >= minSup).keys.toList

      // add frequent itemsets to list
      freqItemsets ++= candidateItemsets

    }

    //  // get itemsets that contain the input
    //  val inputSet = input.split(",").toSet
    //  val relevantItemsets = freqItemsets.filter(_.intersect(inputSet) == inputSet)
    //
    //  // get the item with highest frequency count among relevant itemsets
    //  val itemCounts = mutable.Map[String, Int]().withDefaultValue(0)
    //  for (itemset <- relevantItemsets) {
    //    for (item <- itemset) {
    //      itemCounts(item) += 1
    //    }
    //  }
    //
    //  val highestFreqItem = itemCounts.filterKeys(!inputSet.contains(_)).maxBy(_._2)._1

    //  highestFreqItem
    freqItemsets
  }

  val result = gsp("/Users/mohith.kamanuru/Desktop/OnlineRetailRecommendationSystem/Producer/untitled/src/main/scala/producer/online.retail.log", 7500)
  print(result.length     )

}