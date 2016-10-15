import ch.epfl.lts2.Utils._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scala.math.log

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
      .config("spark.driver.maxResultSize", "10g")
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
    println(wordsTFIDF.take(2).mkString("\n"))

    val wordsImportant = wordsTFIDF.map(text => text.filter { case (word: String, tfidf: Double) => tfidf > text.values.sum / text.size })
    println(wordsImportant.take(2).mkString("\n"))

    println("Preparing vocabulary...")

    val vocabulary = wordsImportant
      .map(text => text
        .map { case (word, tfidf) => word })
      .flatMap(w => w)
      .distinct()
      .collect()
      .toList

    println(vocabulary.contains("lunatic"))
    val vocabLength = vocabulary.length
    println("Vocabulary length: " + vocabLength)


// @TODO Filter texts based on extracted vocabulary. Assign tfidf coefficients to each word in a text in order to extract time series of tfidf coefficients.

    
    println("Windowing texts...")
    val windowedTexts = wordsData.map(_.toList).collect.map(_.sliding(20, 20).toList)

    println("Extracting time series...")
    var i = 0
    for (text <- windowedTexts) {
      i = i + 1
      writeDenseTimeSeries(text, vocabulary, vocabLength, "text" + i)
    }

    println("Transposing dataset...")
    val tsRDD = sc.textFile("./denseTs/text*.txt")
    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr)).cache()
    val trRDD = sc.parallelize(rowRDD.map(_.toSeq).collect.toSeq.transpose)

    println("Preparing vertices for GraphX...")
    //    val verticesList = trRDD.zipWithIndex().map(ts => (ts._2.toLong, (vocabulary(ts._2.toInt), ts._1.toList)))
    val verticesList = trRDD.zipWithIndex().map(ts => (ts._2.toLong, (vocabulary(ts._2.toInt), Vectors.dense(ts._1.map(_.toString.toDouble).toArray).toSparse.indices.toSeq)))

    println("Preparing edges for GraphX...")
    //Construct edges list for GraphX from vocabulary
    val edgeIndexes = for (i <- 0 to vocabulary.length - 1) yield i.toLong
    val edgeIndexesRDD = sc.parallelize(edgeIndexes)
    val edgesList = edgeIndexesRDD.cartesian(edgeIndexesRDD)
      .filter { case (a, b) => a < b }
      .map(pair => Edge(pair._1, pair._2, 1.0))

    println("Releasing memory.")
    trRDD.unpersist()
    rowRDD.unpersist()
    tsRDD.unpersist()
    wordsData.unpersist()
    text.unpersist()
    fullRDD.unpersist()

    /**
      * GraphX
      */
    // Construct a graph
    type Vertex = (String, Seq[Int])

    println("Constructing complete Graph...")
    val vertices: RDD[(VertexId, Vertex)] = verticesList
    val edges: RDD[Edge[Double]] = edgesList

    val graph = Graph(vertices, edges)

    println("Training complete graph with " + edgesList.count() + " edges...")
    val trainedGraph = graph.mapTriplets(triplet => triplet.dstAttr._2.intersect(triplet.srcAttr._2).length).mapVertices((vID, attr) => attr._1).cache()
    println("Removing low weight edges...")
    val prunedGraph = removeLowWeightEdges(trainedGraph, minWeight = 20).cache()
    println("Filtered graph with " + prunedGraph.edges.count() + " edges.")
    println("Removing sigletone vertices...")
    val connectedGraph = removeSingletons(prunedGraph).cache()
    println(connectedGraph.vertices.count() + " vertices remain.")

    println("Saving graph...")
    saveGraph(connectedGraph, "oneCategory.gexf")
    ////    toCSV(graphWithoutZeroEdges)
  }
}
