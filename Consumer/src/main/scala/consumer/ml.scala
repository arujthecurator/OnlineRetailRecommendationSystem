import scala.collection.mutable.ListBuffer
import scala.io.Source

object GSPAlgorithm {
  def main(args: Array[String]): Unit = {
    val inputFileName = "input.txt"
    val minSupport = 2

    val input: List[List[String]] = Source.fromFile(inputFileName).getLines.map(line => line.trim.split("\\s+").toList).toList

    val frequentItems = findFrequentItems(input, minSupport)

    println(s"Frequent items: ${frequentItems.mkString(", ")}")

    val cart: List[String] = List("a", "b")

    val recommendations = findRecommendations(cart, frequentItems, minSupport)

    println(s"Recommendations: ${recommendations.mkString(", ")}")
  }

  def findFrequentItems(data: List[List[String]], minSupport: Int): List[List[String]] = {
    var candidate1: ListBuffer[List[String]] = ListBuffer.empty
    var frequentItems: ListBuffer[List[String]] = ListBuffer.empty

    // Find 1-item sequences
    for (transaction <- data; item <- transaction) {
      if (!candidate1.contains(List(item))) {
        candidate1 += List(item)
      }
    }

    // Find frequent 1-item sequences
    for (itemset <- candidate1) {
      val freq = calcFrequency(data, itemset)
      if (freq >= minSupport) {
        frequentItems += itemset
      }
    }

    var k = 2
    var candidateK: ListBuffer[List[String]] = frequentItems
    var prunedCandidateK: ListBuffer[List[String]] = candidateK

    // Find k-item sequences
    while (prunedCandidateK.nonEmpty) {
      var candidateKPlus1: ListBuffer[List[String]] = ListBuffer.empty
      for (i <- prunedCandidateK.indices; j <- (i + 1) until prunedCandidateK.size) {
        if (prunedCandidateK(i).drop(1) == prunedCandidateK(j).dropRight(1)) {
          val newCandidate = prunedCandidateK(i) :+ prunedCandidateK(j).last
          candidateKPlus1 += newCandidate
        }
      }
      prunedCandidateK = ListBuffer.empty
      for (c <- candidateKPlus1) {
        val freq = calcFrequency(data, c)
        if (freq >= minSupport) {
          frequentItems += c
          prunedCandidateK += c
        }
      }
      candidateK = candidateKPlus1
      k += 1
    }

    frequentItems.toList
  }

  def calcFrequency(data: List[List[String]], itemset: List[String]): Int = {
    data.count(t => itemset.forall(t.contains))
  }

  def findRecommendations(cart: List[String], frequentItems: List[List[String]], minSupport: Int): List[String] = {
    val recommendation: ListBuffer[String] = ListBuffer.empty

    for (itemset <- frequentItems.reverse) {
      if (itemset.length > cart.length && itemset.take(cart.length) == cart) {
        val missingItem = itemset.drop(cart.length).head
        if (!recommendation.contains(missingItem)) {
          recommendation += missingItem
        }
      }
    }

    if (recommendation.isEmpty) {
      // If no recommendations found from frequent items, use update algorithm
      val updateRecommendations = updateRecommendation(cart, frequentItems, minSupport)
      if (updateRecommendations.nonEmpty) {
        recommendation ++= updateRecommendations
      }
    }

    recommendation.toList