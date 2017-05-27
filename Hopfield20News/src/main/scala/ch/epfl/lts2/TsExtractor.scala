package ch.epfl.lts2

import java.io.File

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.immutable.ListMap
import scala.math._
import scala.reflect.io.Path
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
  * Created by volodymyrmiz on 16.10.16.
  */
object TsExtractor {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Time Series Extractor")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "30g")
      .config("spark.executor.cores", "5")
      .config("spark.executor.memory", "30g")
      .getOrCreate()

    val sc = spark.sparkContext

    val windowSize = 20000
    val numFrequentWords = 1000
    val fractionOfUselessWords = 0.99

    logger.info("Reading data...")
    val fullRDD = sc.wholeTextFiles(PATH_DATASET).cache()
    //    val fullRDD = sc.wholeTextFiles("./data/cnn/stories/*").cache()

    val text = fullRDD.map { case (file, text) => text }.cache()

    /**
      * Tokenization. Split raw text content in each document into a collection of terms. Get vocabulary.
      */
    logger.info("Removing emails and URLs...")
    val rxEmail = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r
    val rxWebsite = """([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?""".r
    val rxNumbers = """[^0-9]*""".r

    val textWOEmails = text
      .map(t => rxEmail.replaceAllIn(t, ""))
      .map(t => rxWebsite.replaceAllIn(t, ""))

    val stopWords = StopWordsRemover.loadDefaultStopWords("english")
    val frequentWords = sc.textFile("./frequentWords.txt").take(numFrequentWords).toList

    logger.info("Cleaning texts. Tokenization...")
    val wordsData = textWOEmails.map(t => t.split("""\W+"""))
      .map(t => t.map(_.toLowerCase.replaceAll("_", ""))
        .filter(token => rxNumbers.pattern.matcher(token).matches() & token.length() > 2 & !stopWords.contains(token) & !frequentWords.contains(token)))
//        .filter(token => rxNumbers.pattern.matcher(token).matches() & token.length() > 1)) // with stopwords
//        .filter(token => rxNumbers.pattern.matcher(token).matches() & token.length() > 2 & !stopWords.contains(token))) // with frequent words

    logger.info("TF...")
    val wordsTF = wordsData
      .map(text => text.groupBy(word => word).map { case (word, num) => (word, num.length) })
      .map(text => text.map { case (word, numOccur) => (word, numOccur / text.values.max.toDouble) })

    logger.info("IDF...")
    val nDocs = wordsData.count()
    val wordsIDF = wordsData
      .map(text => text.distinct)
      .flatMap(w => w)
      .groupBy(w => w)
      .map { case (word, numOccur) => (word, log(nDocs / numOccur.size) / log(2)) }.collect().toList.toMap

    logger.info("TFIDF...")
    val wordsTFIDF = wordsTF.map(text => text.map { case (word, tf) => (word, tf * wordsIDF(word)) })

    // Take a fraction of most significant TFIDFs
    val wordsImportant = wordsTFIDF.map(text => ListMap(text.toSeq.sortBy(_._2): _*).drop((text.size * fractionOfUselessWords).toInt))

    logger.info("Preparing vocabulary...")
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

    logger.info("Vocabulary length: " + vocabLength)

    logger.info("Zipping text with TFIDF coefficients... ")
    val zippedRDD = wordsData.map(_.toList).map(_.filter(vocabList.contains(_))).zip(wordsTFIDF)
    val wordsDataIndexedByTFIDF = zippedRDD
      .map { case (list, tfidfMap) => list.map(word => (word, tfidfMap(word)))
      }
    wordsDataIndexedByTFIDF.count()

    logger.info("Windowing texts...")
    val windowedTexts = sc.parallelize(wordsDataIndexedByTFIDF.collect.map(_.sliding(windowSize, windowSize).toList)).map(list => list.map(_.toMap))
    windowedTexts.count()

    logger.info("Generating time series...")
    val ts = windowedTexts.flatMap(data => data).map(text => writeTS(text, vocabList, vocabLength)).filter(_.numNonzeros > 0).map(_.toDense.toArray).collect()

    logger.info("Transposing TS...")
    val transposed = ts.transpose

    logger.info("Saving TS...")
    val trRDD = sc.parallelize(transposed.map(Vectors.dense(_).toSparse))
    Path(PATH_OUTPUT + "trRDD").deleteRecursively()
    trRDD.zipWithIndex().saveAsObjectFile(PATH_OUTPUT + "trRDD")


    import java.io._

    val file = PATH_OUTPUT + "transposed.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- transposed) {
      writer.write(x.mkString(",") + "\n")
    }
    writer.close()


  }
}
