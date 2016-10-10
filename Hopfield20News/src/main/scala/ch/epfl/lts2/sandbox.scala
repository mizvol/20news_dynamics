package ch.epfl.lts2

import java.sql.Struct

import ch.epfl.lts2.Utils._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.graphx._
import java.io._

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by volodymyrmiz on 05.10.16.
  */
object sandbox {
  def main(args: Array[String]): Unit = {

    suppressLogs(List("org", "akka"))
    /*
    Create Spark Session and define Spark Context
     */
    val spark = SparkSession.builder
      .master("local")
      .appName("Sandbox")
      .config("spark.sql.warehouse.dir", "../")
      .getOrCreate()

    val sc = spark.sparkContext

    def writeTimeSeries(oneText: List[List[String]], vocabulary: List[String], vocabLength: Int, fileName: String) = {
      val pw = new PrintWriter(new File("./testSamples/" + fileName + ".txt"))
      for (window <- oneText) {
        val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
        val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1)).toDense.toString()
        pw.write(vector.substring(1, vector.length - 1) + "\n")
      }
      pw.close()
    }

    val doc = List(
      List("our", "text", "template", "aimed", "to", "extract", "time", "series", "from", "text"),
      List("another", "text", "template", "in", "order", "to", "extract", "series", "from", "text"))

    //    val windowedDoc = doc.sliding(3, 3)

    val windowedDoc = doc.map(_.sliding(3, 3).toList)

//    val vocabulary = List("text", "template", "aimed", "extract", "time", "series", "another", "order")
    val vocabulary = List("text", "template", "aimed", "extract", "time", "series")

    val vocabLength = vocabulary.length

    var i = 0
    for (text <- windowedDoc) {
      i = i + 1
      writeTimeSeries(text, vocabulary, vocabLength, "text" + i)
    }

    /** *
      * DataFrame of time series
      */
    val fields = vocabulary.map(StructField(_, StringType, nullable = true))
    val schema = StructType(fields)

    val tsRDD = spark.sparkContext.textFile("./testSamples/text1.txt")

    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr))
    val tsDF = spark.createDataFrame(rowRDD, schema)
    tsDF.show()

    //Construct vertices list for GraphX from DataFrame
    val verticesList =
    for ((column, count) <- tsDF.columns.zipWithIndex)
      yield (count.toLong, (column.toString, tsDF.select(column).rdd.map(_ (0)).collect.toList))

    //Construct edges list for GraphX from vocabulary
    val edgeIndexes = for (i <- 0 to vocabulary.length - 1) yield i.toLong
    val edgeIndexesRDD = sc.parallelize(edgeIndexes)
    val edgesList = edgeIndexesRDD.cartesian(edgeIndexesRDD)
      .filter { case (a, b) => a < b }
      .map(pair => Edge(pair._1, pair._2, 1.0))
      .collect.toList

    /** *
      * GraphX
      */
    // Construct a graph
    type Vertex = (String, List[Any])

    val vertices: RDD[(VertexId, Vertex)] = sc.parallelize(verticesList)
    val edges: RDD[Edge[Double]] = sc.parallelize(edgesList)

    val graph = Graph(vertices, edges)
    println(graph.triplets.collect.mkString("\n"))

    def compareLists(l1: List[Any], l2: List[Any]) = l1.zip(l2).count({case (x,y) => x.toString.toDouble == 1.0 & y.toString.toDouble == 1.0})

    val trainedGraph = graph.mapTriplets(triplet => compareLists(triplet.dstAttr._2, triplet.srcAttr._2))
    println(trainedGraph.edges.collect.mkString("\n"))

    val pw = new PrintWriter("myGraph.gexf")
    pw.write(toGexf(trainedGraph))
    pw.close
  }
}