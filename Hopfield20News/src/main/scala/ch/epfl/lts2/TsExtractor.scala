package ch.epfl.lts2

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._

import scala.collection.immutable.ListMap
import scala.math._

/**
  * Created by volodymyrmiz on 16.10.16.
  */
object TsExtractor {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))
    /*
    Create Spark Session and define Spark Context
     */
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Time Series Extractor")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.executor.cores", "1")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    val sc = spark.sparkContext

    println("Reading data...")
    val fullRDD = sc.wholeTextFiles("./data/20news-bydate-train/*").cache()

    val text = fullRDD.map { case (file, text) => text }.cache()

    /**
      * Tokenization. Split raw text content in each document into a collection of terms. Get vocabulary.
      */
    println("Removing emails and URLs...")
    val rxEmail = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r
    val rxWebsite = """([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?""".r
    val rxNumbers = """[^0-9]*""".r

    val textWOEmails = text
      .map(t => rxEmail.replaceAllIn(t, ""))
      .map(t => rxWebsite.replaceAllIn(t, ""))

    val stopWords = StopWordsRemover.loadDefaultStopWords("english")

    println("Cleaning texts. Tokenization...")
    val wordsData = textWOEmails.map(t => t.split("""\W+"""))
      .map(t => t.map(_.toLowerCase.replaceAll("_", ""))
        .filter(token => rxNumbers.pattern.matcher(token).matches() & token.length() > 2 & !stopWords.contains(token)))

    println("TF...")
    val wordsTF = wordsData
      .map(text => text.groupBy(word => word).map { case (word, num) => (word, num.length) })
      .map(text => text.map { case (word, numOccur) => (word, numOccur / text.values.max.toDouble) })

    println("IDF...")
    val nDocs = wordsData.count()
    val wordsIDF = wordsData
      .map(text => text.distinct)
      .flatMap(w => w)
      .groupBy(w => w)
      .map { case (word, numOccur) => (word, log(nDocs / numOccur.size) / log(2)) }.collect().toList.toMap

    println("TFIDF...")
    val wordsTFIDF = wordsTF.map(text => text.map { case (word, tf) => (word, tf * wordsIDF.get(word).get) })

    //    Take words with TFIDF higher than mean value
    //    val wordsImportant = wordsTFIDF.map(text => text.filter { case (word: String, tfidf: Double) => tfidf > text.values.sum / text.size })

    // Take a fraction of most significant TFIDFs
    val wordsImportant = wordsTFIDF.map(text => ListMap(text.toSeq.sortBy(_._2): _*).drop((text.size * 0.9).toInt))

    println("Preparing vocabulary...")
    val vocabRDD = wordsImportant
      .map(text => text
        .map { case (word, tfidf) => word })
      .flatMap(w => w)
      .distinct()

    vocabRDD.zipWithIndex().saveAsObjectFile("./data/vocabRDD")

    val vocabList = vocabRDD
      .collect()
      .toList

    val vocabLength = vocabList.length

    println("Vocabulary length: " + vocabLength)

    println("Zipping text with TFIDF coefficients... ")
    val zippedRDD = wordsData.map(_.toList).zip(wordsTFIDF)
    val wordsDataIndexedByTFIDF = zippedRDD
      .map { case (list, tfidfMap) => (list
        .map(word => (word, tfidfMap.get(word).get)))
      }

    println("Windowing texts...")
    val windowedTexts = sc.parallelize(wordsDataIndexedByTFIDF.collect.map(_.sliding(20, 20).toList).map(list => list.map(_.toMap))).zipWithIndex()

    println("Generating time series...")
    windowedTexts.map { case (list, index) => writeDenseTimeSeries(list, vocabList, vocabLength, "text" + index) }.count()
  }
}
