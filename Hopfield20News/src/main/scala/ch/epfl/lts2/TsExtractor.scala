package ch.epfl.lts2

import java.io.File

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.immutable.ListMap
import scala.math._
import scala.reflect.io.Path

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

    val windowSize = 200
    val numFrequentWords = 5000

    println("Reading data...")
    val fullRDD = sc.wholeTextFiles(PATH_DATASET).cache()
    //    val fullRDD = sc.wholeTextFiles("./data/cnn/stories/*").cache()

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
    val frequentWords = sc.textFile("./frequentWords.txt").take(numFrequentWords).toList

    println("Cleaning texts. Tokenization...")
    val wordsData = textWOEmails.map(t => t.split("""\W+"""))
      .map(t => t.map(_.toLowerCase.replaceAll("_", ""))
//        .filter(token => rxNumbers.pattern.matcher(token).matches() & token.length() > 2 & !stopWords.contains(token) & !frequentWords.contains(token)))
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
    val wordsTFIDF = wordsTF.map(text => text.map { case (word, tf) => (word, tf * wordsIDF(word)) })

    println(wordsTFIDF.count())

//    println(wordsTFIDF.take(5).mkString(", "))
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

    Path(PATH_OUTPUT + "vocabRDD").deleteRecursively()
    vocabRDD.zipWithIndex().saveAsObjectFile(PATH_OUTPUT + "vocabRDD")

    val vocabList = vocabRDD
      .collect()
      .toList

    val vocabLength = vocabList.length

    println("Vocabulary length: " + vocabLength)

    println("Zipping text with TFIDF coefficients... ")
    val zippedRDD = wordsData.map(_.toList).zip(wordsTFIDF)
    val wordsDataIndexedByTFIDF = zippedRDD
      .map { case (list, tfidfMap) => list.filter(word => vocabList.contains(word))
        .map(word => (word, tfidfMap(word)))
      }

    wordsDataIndexedByTFIDF.count()
    println("Windowing texts...")
//    val windowedTexts = sc.parallelize(wordsDataIndexedByTFIDF.collect.map(_.sliding(windowSize, windowSize).toList).map(list => list.map(_.toMap))).zipWithIndex()

    val windowedTexts = sc.parallelize(wordsDataIndexedByTFIDF.collect.map(_.sliding(windowSize, windowSize).toList).map(list => list.map(_.toMap)))
//    println(windowedTexts.take(5).mkString(", "))

    windowedTexts.count()
    def writeTS(text: Map[String, Double], vocabulary: List[String], vLength: Int): SparseVector = {
        val indexes = vocabulary.filter(text.keys.toList.contains(_)).map(word => (vocabulary.indexOf(word), text(word)))
        Vectors.sparse(vLength, indexes.map(_._1).toArray, indexes.map(_._2).toArray).toSparse
    }

    println("Generating time series...")
    val ts = windowedTexts.map(text => text.map(writeTS(_, vocabList, vocabLength)).filter(_.numNonzeros > 0)).flatMap(data => data).map(_.toDense.toArray).collect()
//    println(ts.take(10).mkString(", "))

    println("Transposing TS...")
    val transposed = ts.transpose
//    println(transposed.take(5).map(_.mkString(",")).mkString(", "))

    import java.io._
    val pw = new PrintWriter(new File("transposed.xlsx"))
    transposed.map(row => pw.write(row.mkString(",") + "\n"))
    pw.close()
    val pw_ = new PrintWriter(new File("raw.xlsx"))
    ts.map(row => pw_.write(row.mkString(",") + "\n"))
    pw_.close()

    println("Saving TS...")
    val trRDD = sc.parallelize(transposed.map(Vectors.dense(_).toSparse))
    Path(PATH_OUTPUT + "trRDD").deleteRecursively()
    trRDD.zipWithIndex().saveAsObjectFile(PATH_OUTPUT + "trRDD")

//    println("Generating time series...")
//    for {
//      files <- Option(new File(PATH_OUTPUT + "denseTS").listFiles)
//      file <- files if file.exists()
//    } file.delete()
////    windowedTexts.map { case (list, index) => writeDenseTimeSeries(list, vocabList, vocabLength, PATH_OUTPUT + "denseTS/text" + index + ".txt") }.count()
//    windowedTexts.map { case (list, index) => writeSparseTimeSeries(list, vocabList, vocabLength, PATH_OUTPUT + "sparseTS/text" + index + ".txt") }.count()
  }
}
