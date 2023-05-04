package consumer

import scala.collection.mutable
import scala.io.Source
import java.io.{File, PrintWriter}

object ML extends App {

  /**
   * Computes the most frequent sets of items using the Generalized Sequential Pattern algorithm.
   *
   * @param logFile the path to the input data file
   * @param minSupport the minimum support threshold
   * @return a list of the most frequent itemsets in the data
   */
  def gsp(logFile: String, minSupport: Int): List[Set[String]] = {
    // Set up output log file path
    val resultLogPath = "src/main/scala/consumer/results.log"
    // Read input file and group transactions by invoice number
    val transactions = Source.fromFile(logFile).getLines().toList.map(_.split(","))
    val groupedTransactions = transactions.groupBy(_.head).values.toList.map(_.map(_ (2)).sorted)
    // Compute the frequency of each item in the transactions
    val itemFreq = mutable.Map[String, Int]().withDefaultValue(0)
    for (transaction <- groupedTransactions) {
      for (item <- transaction) {
        itemFreq(item) += 1
      }
    }
    // Filter the itemsets that meet the minimum support threshold
    val freqItems = itemFreq.filter(_._2 >= minSupport).keys.toSet
    // Initialize candidate itemsets with 1-itemsets
    var candidateItemsets = freqItems.map(Set(_)).toList
    // Filter candidate itemsets that do not meet the minimum support threshold
    var freqItemsets = candidateItemsets.filter(itemset => itemset.forall(freqItems.contains))
    // Loop until there are no more candidate itemsets
    while (candidateItemsets.nonEmpty) {
      // Generate new candidate itemsets
      val newCandidateItemsets = candidateItemsets.flatMap(itemset => freqItems.filter(!itemset.contains(_)).map(item => itemset + item))
      // Compute the support for each candidate itemset
      val candidateSupport = mutable.Map[Set[String], Int]().withDefaultValue(0)
      for (transaction <- groupedTransactions) {
        for (candidateItemset <- newCandidateItemsets) {
          if (candidateItemset.subsetOf(transaction.toSet)) {
            candidateSupport(candidateItemset) += 1
          }
        }
      }
      // Filter candidate itemsets that do not meet the minimum support threshold
      candidateItemsets = candidateSupport.filter(_._2 >= minSupport).keys.toList
      // Add frequent itemsets to list
      freqItemsets ++= candidateItemsets
    }
    // Empty existing results (if any) in output log file
    val resultFile = new File(resultLogPath)
    if (resultFile.exists() && resultFile.length() > 0) {
      val writer = new PrintWriter(resultFile)
      writer.print("")
      writer.close()
    }
    // Save the frequent itemsets to the output log file
    val writer = new PrintWriter(resultFile)
    freqItemsets.foreach(set => writer.println(set.mkString(",")))
    writer.close()
    freqItemsets
  }


    /**
   * Computes the most frequent sets of items using Generalized Sequential Pattern algorithm.
   *
   * @param inputSet  input Set of Items for which the recommendation needs to be computed
   * @return A list of Recommendations
   */
  def recommendation(inputSet: Set[String]): List[String] = {
    // Check if the result log file exists, if not return an empty list
    val resultLogPath = "src/main/scala/consumer/results.log"
    val resultFile = new File(resultLogPath)
    if (!resultFile.exists()) return List.empty[String]

    // Read frequent itemsets from the result log file and filter by the input set
    val sets = Source.fromFile(resultLogPath).getLines().map(line => line.split(",").toSet)
    val frequentList = sets.toList
    val relevantItemsets = frequentList.filter(set => inputSet.subsetOf(set))
    if (relevantItemsets.isEmpty) return List.empty[String]


    // Calculate item frequencies in the relevant itemsets and find the items with the highest frequency that are not in the input set
    val itemCounts = mutable.Map[String, Int]().withDefaultValue(0)
    for (itemset <- relevantItemsets) {
      for (item <- itemset) {
        itemCounts(item) += 1
      }
    }
    // Find all the Items that are not in the InputSet, and have the Maximum Frequency Value
    if(itemCounts.filterKeys(!inputSet.contains(_)).isEmpty) return List.empty[String]
    val maxFreq = itemCounts.filterKeys(!inputSet.contains(_)).values.max
    val highestFreqItems = itemCounts.filterKeys(!inputSet.contains(_)).filter(_._2 == maxFreq).keys.toList
    highestFreqItems
  }

  //  For Giving the recommendation, remove the comment from the following snippet
  val hardcodedInput = Set("LUNCH BAG  BLACK SKULL.","LUNCH BAG RED RETROSPOT")
  val ans = recommendation(hardcodedInput)
  print("Recommended Items for the Input Cart: ")
  println(ans)
}