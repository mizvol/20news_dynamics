package ch.epfl.lts2

import java.io.PrintWriter

import ch.epfl.lts2.Utils._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD

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

    val doc = List(
      List("our", "text", "template", "aimed", "to", "extract", "time", "series", "from", "text"),
      List("another", "text", "template", "in", "order", "to", "extract", "series", "from", "text"))

    //    val windowedDoc = doc.sliding(3, 3)

    val windowedDoc = doc.map(_.sliding(3, 3).toList)

    val vocabulary = List("text", "template", "aimed", "extract", "time", "series", "another", "order")
//    val vocabulary = List("text", "template", "aimed", "extract", "time", "series")

    val vocabLength = vocabulary.length

    var i = 0
    for (text <- windowedDoc) {
      i = i + 1
      writeDenseTimeSeries(text, vocabulary, vocabLength, "text" + i)
    }

    val tsRDD = sc.textFile("./denseTs/text1.txt")

    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr))

    val trRDD = sc.parallelize(rowRDD.map(_.toSeq).collect.toSeq.transpose)

    val verticesList = trRDD.zipWithIndex().map(ts => (ts._2.toLong, (vocabulary(ts._2.toInt), Vectors.dense(ts._1.map(_.toString.toDouble).toArray).toSparse.indices.toSeq)))
    println(verticesList.collect().mkString(" "))

//    val tsDF = spark.createDataFrame(rowRDD, schema)
//
//    val verticesList =
//      for ((column, count) <- tsDF.columns.zipWithIndex)
//        yield (count.toLong, (column.toString, tsDF.select(column).rdd.map(_ (0)).collect.toList))

    // Construct vertices for GraphX from RDD with TimeSeries in columns
//    val verticesList = for (i <- 0 to vocabLength - 1)
//      yield (i.toLong, (vocabulary(i), rowRDD.map(row => row(i)).collect.toList))
//
    //Construct edges list for GraphX from vocabulary
    val edgeIndexes = for (i <- 0 to vocabulary.length - 1) yield i.toLong
    val edgeIndexesRDD = sc.parallelize(edgeIndexes)
    val edgesList = edgeIndexesRDD.cartesian(edgeIndexesRDD)
      .filter { case (a, b) => a < b }
      .map(pair => Edge(pair._1, pair._2, 1.0))

    /**
      * GraphX
      */
    // Construct a graph
    type Vertex = (String, Seq[Int])

    val vertices: RDD[(VertexId, Vertex)] = verticesList
    val edges: RDD[Edge[Double]] = edgesList

    val graph = Graph(vertices, edges)

    val trainedGraph = graph.mapTriplets(triplet => triplet.dstAttr._2.intersect(triplet.srcAttr._2).length).mapVertices((vID, attr) => attr._1)

    val graphWithoutZeroEdges = removeLowWeightEdges(trainedGraph, 0)

    val connectedGraph = removeSingletons(graphWithoutZeroEdges)

    saveGraph(connectedGraph, "myGraph.gexf")

    toCSV(connectedGraph)
  }
}
