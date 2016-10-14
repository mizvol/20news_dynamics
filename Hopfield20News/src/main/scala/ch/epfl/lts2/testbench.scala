import ch.epfl.lts2.Utils._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

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
      .config("spark.driver.maxResultSize", "7g")
      .getOrCreate()

    val sc = spark.sparkContext

    val fullRDD = sc.wholeTextFiles("./data/20news-bydate-train/*").cache()

    val text = fullRDD.map { case (file, text) => text }.cache()

    /**
      * Tokenization. Split raw text content in each document into a collection of terms. Get vocabulary.
      */
    println("Tokenization started...")
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase())).cache()

    val regexp = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regexp.pattern.matcher(token).matches()).cache()

    //  Frequent words
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
    println("Unique words: " + tokenCounts.collect().length)

    println("Preparing vocabulary...")
    val frequentWords = tokenCounts.sortBy(-_._2).map(_._1).take(20)
    val rareWords = tokenCounts.filter(_._2 < 10).map(_._1).collect()
    val filterWords = filterNumbers.distinct().filter(word => !rareWords.contains(word) & !frequentWords.contains(word) & word.length() > 3)
    val vocabulary = filterWords.collect.toList
    println("Vocabulary length: " + vocabulary.length)

    println("Preparing texts...")
    val wordsOnly = text.map(t => t.split("""\W+"""))
      .map(t => t
        .filter(token => regexp.pattern.matcher(token).matches() & token.length() > 2))
      .map(t => t
        .map(_.toLowerCase))

    val windowedTexts = wordsOnly.map(_.toList).collect.map(_.sliding(20, 20).toList)

    val vocabLength = vocabulary.length

    println("Extracting time series...")
    var i = 0
    for (text <- windowedTexts) {
      i = i + 1
      writeDenseTimeSeries(text, vocabulary, vocabLength, "text" + i)
    }

    println("Preparing vertices for GraphX...")
    val tsRDD = sc.textFile("./denseTs/text*.txt")
    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr)).cache()
    val trRDD = sc.parallelize(rowRDD.map(_.toSeq).collect.toSeq.transpose)

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
    wordsOnly.unpersist()
    text.unpersist()
    fullRDD.unpersist()
    nonWordSplit.unpersist()

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
//    toCSV(graphWithoutZeroEdges)
  }
}
