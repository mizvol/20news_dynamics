import java.io.{File, PrintWriter}

import ch.epfl.lts2.Utils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by volodymyrmiz on 05.10.16.
  */
object Testbench {
  def main(Args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))
    /*
    Create Spark Session and define Spark Context
     */
    val spark = SparkSession.builder
      .master("local")
      .appName("Hopfileld Filtering 20NEWS")
      .config("spark.sql.warehouse.dir", "../")
      .getOrCreate()

    val sc = spark.sparkContext

    val fullRDD = sc.wholeTextFiles("./data/20news-bydate-train/*").cache()

    val text = fullRDD.map { case (file, text) => text }.cache()

    /** *
      * Tokenization. Split raw text content in each document into a collection of terms. Get vocabulary.
      */

    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase())).cache()
    //
    val regexp =
    """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regexp.pattern.matcher(token).matches()).cache()

    ////  Frequent words
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)

    val frequentWords = tokenCounts.sortBy(-_._2).map(_._1).take(20)

    val filterFrequentWords = filterNumbers.filter(word => !frequentWords.contains(word) & word.length() > 2).distinct
    val vocabulary = filterFrequentWords.collect.toList

    /** *
      * End of tokenization
      */

    //    val vocabulary = List("okcforum", "thoughts", "version", "sandvik", "wrote", "someone", "originate", "anyway")

    /** *
      * Prepare texts
      */
    val wordsOnly = text.map(t => t.split("""\W+"""))
      .map(t => t
        .filter(token => regexp.pattern.matcher(token).matches() & token.length() > 2))
      .map(t => t
        .map(_.toLowerCase))

    val windowedTexts = wordsOnly.map(_.toList).collect.map(_.sliding(10, 10).toList)

//    val oneText = windowedTexts.take(1)(0)

    /** *
      * End of prepare text
      */

    val vocabLength = vocabulary.length

    /** *
      * Get time series
      */
    def writeTimeSeries(oneText: List[List[String]], fileName: String) = {
      val pw = new PrintWriter(new File("./ts/" + fileName + ".txt"))
      for (window <- oneText) {
        val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
        val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1)).toString()
        pw.write(vector.substring(1, vector.length - 1) + "\n")
      }
      pw.close()
    }

    var i = 0
    for(text <- windowedTexts) {
      i = i+1
      writeTimeSeries(text, "text" + i)
    }

  }
}
