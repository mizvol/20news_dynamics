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
      g.edges.map(e => if (!List(1 to 10).contains(e.attr)) "        <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "        </edges>\n" +
      " </graph>\n" +
      "</gexf>"

  def toCSV[VD, ED](graph: Graph[VD, ED]) = {
    val pwv = new PrintWriter("vertices.csv")
    pwv.write("Id,Label\n")
    for (vertex <- graph.vertices.collect()){
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

  def writeDenseTimeSeries(oneText: List[List[String]], vocabulary: List[String], vocabLength: Int, fileName: String) = {
    val pw = new PrintWriter(new File("./denseTs/" + fileName + ".txt"))
    for (window <- oneText) {
      val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
      val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1)).toDense.toString()
      pw.write(vector.substring(1, vector.length - 1) + "\n")
    }
    pw.close()
  }

  def writeSparseTimeSeries(oneText: List[List[String]], vocabulary: List[String], vocabLength: Int, fileName: String) = {
    val pw = new PrintWriter(new File("./sparseTs/" + fileName + ".txt"))
    for (window <- oneText) {
      val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
      val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1))
      pw.write(vector + "\n")
    }
    pw.close()
  }

  def removeLowWeightEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], minWeight: Int) = {
    Graph(graph.vertices,
      graph.edges.filter(_.attr.toString.toInt > minWeight)
    )
  }

  def removeSingletons[VD: ClassTag,ED: ClassTag](graph:Graph[VD,ED]) =
    Graph(graph.triplets.map(et => (et.srcId,et.srcAttr))
      .union(graph.triplets.map(et => (et.dstId,et.dstAttr)))
      .distinct,
      graph.edges)
}
