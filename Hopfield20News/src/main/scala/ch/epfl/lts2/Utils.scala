package ch.epfl.lts2

import java.io.{File, PrintWriter}
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.linalg.Vectors
import scala.reflect.ClassTag

/**
  * Created by volodymyrmiz on 05.10.16.
  */
package object Utils {
  /** *
    * Hide Apache Spark console logs.
    *
    * @param params List of logs to be suppressed.
    */
  def suppressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
  }

  def toGexf[VD, ED](g: Graph[VD, ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      " <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "      </nodes>\n" +
      "      <edges>\n" +
      g.edges.map(e => "        <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "        </edges>\n" +
      " </graph>\n" +
      "</gexf>"

  def toCSV[VD, ED](graph: Graph[VD, ED]) = {
    val pwv = new PrintWriter("vertices.csv")
    pwv.write("Id,Label\n")
    for (vertex <- graph.vertices.collect()) {
      pwv.write(vertex._1.toString() + "," + vertex._2.toString() + "\n")
    }
    pwv.close()

    val pwe = new PrintWriter("edges.csv")
    pwe.write("Source,Target,Type,Weight,Label\n")
    for (edge <- graph.edges.collect()) {
      pwe.write(edge.dstId.toInt + "," + edge.srcId.toInt + "," + "Undirected" + "," + edge.attr.toString.toDouble + "," + edge.attr.toString.toDouble + "\n")
    }
    pwe.close()
  }

  def saveGraph[VD, ED](graph: Graph[VD, ED], fileName: String) = {
    val pw = new PrintWriter(fileName)
    pw.write(toGexf(graph))
    pw.close
  }

  def writeDenseTimeSeries(text: List[Map[String, Double]], vocabulary: List[String], vocabLength: Int, path: String) = {
    val pw = new PrintWriter(new File(path))
    for (window <- text) {
      val indexes = vocabulary.filter(window.keys.toList.contains(_)).map(word => (vocabulary.indexOf(word), window(word)))
      val vector = Vectors.sparse(vocabLength, indexes.map(_._1).toArray, indexes.map(_._2).toArray)
      if (vector.toSparse.indices.length != 0) {
        val vectorString = vector.toDense.toString()
        pw.write(vectorString.substring(1, vectorString.length - 1) + "\n")
      }
    }
    pw.close()
  }

  def writeSparseTimeSeries(text: List[Map[String, Double]], vocabulary: List[String], vocabLength: Int, path: String) = {
    val pw = new PrintWriter(new File(path))
    for (window <- text) {
      val indexes = vocabulary.filter(window.keys.toList.contains(_)).map(word => (vocabulary.indexOf(word), window(word)))
      val vector = Vectors.sparse(vocabLength, indexes.map(_._1).toArray, indexes.map(_._2).toArray)
      if (vector.toSparse.indices.length != 0) {
        pw.write(vector + "\n")
      }
    }
    pw.close()
  }

  def removeLowWeightEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], minWeight: Double) = {
    Graph(graph.vertices,
      graph.edges.filter(_.attr.toString.toDouble > minWeight)
    )
  }

  def removeSingletons[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) =
    Graph(graph.triplets.map(et => (et.srcId, et.srcAttr))
      .union(graph.triplets.map(et => (et.dstId, et.dstAttr)))
      .distinct,
      graph.edges)

  def compareTimeSeries(m1: Map[Int, Double], m2: Map[Int, Double]): Double = {
    val commonKeys = m2.keySet.intersect(m1.keySet)

    if (commonKeys.isEmpty) 0
    else {
      var weight: Double = 0.0
      for (key <- commonKeys) {
        val value1 = m1(key)
        val value2 = m2(key)
        var threshold = 0.0

        if (value1 > value2)
          threshold = value2/value1
        else
          threshold = value1/value2

        if (threshold > 0.5) weight += threshold
        else weight -= threshold
      }
      weight
    }
  }
}
