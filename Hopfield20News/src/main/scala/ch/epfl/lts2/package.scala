package ch.epfl.lts2

import java.io.{File, PrintWriter}

import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.linalg.Vectors

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
      g.edges.map(e => if (e.attr != 0) "        <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "        </edges>\n" +
      " </graph>\n" +
      "</gexf>"

  def compareLTimeSeries(l1: List[Any], l2: List[Any]) =
    l1.zip(l2).count({ case (x, y) => x.toString.toDouble == 1.0 & y.toString.toDouble == 1.0 })

  def saveGraph[VD, ED](graph: Graph[VD, ED], fileName: String) = {
    val pw = new PrintWriter("myGraph.gexf")
    pw.write(toGexf(graph))
    pw.close
  }

  def writeDenseTimeSeries(oneText: List[List[String]], vocabulary: List[String], vocabLength: Int, fileName: String) = {
    val pw = new PrintWriter(new File("./testSamples/" + fileName + ".txt"))
    for (window <- oneText) {
      val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
      val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1)).toDense.toString()
      pw.write(vector.substring(1, vector.length - 1) + "\n")
    }
    pw.close()
  }

  def writeSparseTimeSeries(oneText: List[List[String]], vocabulary: List[String], vocabLength: Int, fileName: String) = {
    val pw = new PrintWriter(new File("./ts/" + fileName + ".txt"))
    for (window <- oneText) {
      val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
      val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1)).toString()
      pw.write(vector.substring(1, vector.length - 1) + "\n")
    }
    pw.close()
  }
}
