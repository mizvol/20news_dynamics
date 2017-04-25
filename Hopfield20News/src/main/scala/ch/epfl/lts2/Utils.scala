package ch.epfl.lts2

import java.io.{File, PrintWriter}

import breeze.linalg.{max, min}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

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

  def writeTS(text: Map[String, Double], vocabulary: List[String], vLength: Int): SparseVector = {
    val indexes = vocabulary.filter(text.keys.toList.contains(_)).map(word => (vocabulary.indexOf(word), text(word)))
    Vectors.sparse(vLength, indexes.map(_._1).toArray, indexes.map(_._2).toArray).toSparse
  }

  def removeLowWeightEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], minWeight: Double) = {
    Graph(graph.vertices,
      graph.edges.filter(_.attr.toString.toDouble > minWeight)
    )
  }

  def removeLowWeightEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {
    val weights = graph.edges.map(e=>(e.attr)).collect.map(_.toString.toDouble)
    val mean = weights.sum.toString.toDouble/weights.length

    Graph(graph.vertices,
      graph.edges.filter(_.attr.toString.toDouble > mean)
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
        val threshold = min(value1, value2)/max(value1, value2)
        if (threshold > 0.5) weight += threshold
        else weight -= threshold
      }
      weight
    }
  }

  def minSpanningTree[VD: ClassTag, ED: ClassTag](g: Graph[VD, Double]) = {
    var g2 = g.mapEdges(e => (e.attr,false))
    for (i <- 1L to g.vertices.count-1) {
      val unavailableEdges =
        g2.outerJoinVertices(g2.subgraph(_.attr._2)
          .connectedComponents
          .vertices)((vid,vd,cid) => (vd,cid))
          .subgraph(et => et.srcAttr._2.getOrElse(-1) ==
            et.dstAttr._2.getOrElse(-2))
          .edges
          .map(e => ((e.srcId,e.dstId),e.attr))
      type edgeType = Tuple2[Tuple2[VertexId,VertexId],Double]
      val smallestEdge =
        g2.edges
          .map(e => ((e.srcId,e.dstId),e.attr))
          .leftOuterJoin(unavailableEdges)
          .filter(x => !x._2._1._2 && x._2._2.isEmpty)
          .map(x => (x._1, x._2._1._1))
          .min()(new Ordering[edgeType]() {
            override def compare(a:edgeType, b:edgeType) = {
              val r = Ordering[Double].compare(a._2,b._2)
              if (r == 0)
                Ordering[Long].compare(a._1._1, b._1._1)
              else
                r
            }
          })
      g2 = g2.mapTriplets(et =>
        (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1
          && et.dstId == smallestEdge._1._2)))
    }
    g2.subgraph(_.attr._2).mapEdges(_.attr._1)
  }

  def getLargestConnectedComponent[VD: ClassTag, ED: ClassTag](g: Graph[VD, Double]): Graph[VD, Double] = {
    val cc = g.connectedComponents()
    val ids = cc.vertices.map((v: (Long, Long)) => v._2)
    val largestId = ids.map((_, 1L)).reduceByKey(_ + _).sortBy(-_._2).keys.collect(){0}
    val largestCC = cc.vertices.filter((v: (Long, Long)) => v._2 == largestId)
    val lccVertices = largestCC.map(_._1).collect()
    g.subgraph(vpred = (id, attr) => lccVertices.contains(id))
  }
}
