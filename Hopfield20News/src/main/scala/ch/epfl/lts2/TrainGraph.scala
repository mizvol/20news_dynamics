import java.util.Calendar

import ch.epfl.lts2.Utils._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import ch.epfl.lts2.Globals._
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
  * Created by volodymyrmiz on 05.10.16.
  */
object TrainGraph {
  def main(Args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Hopfileld Filtering 20NEWS")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.cores", "5")
      .config("spark.executor.memory", "50g")
      .getOrCreate()

    val sc = spark.sparkContext

    val trRDD = sc.objectFile[(SparseVector, Long)](PATH_OUTPUT + "trRDD").sortBy(_._2).map(_._1)

    val vocabulary = sc.objectFile[(String, Long)](PATH_OUTPUT + "vocabRDD").sortBy(_._2).map(_._1).collect().toList

    logger.info("Vocabulary length: " + vocabulary.length)
    logger.info("Preparing vertices for GraphX...")

    val normalizer = new Normalizer()

    val verticesRDD = trRDD.map(activation => normalizer.transform(activation))
      .zipWithIndex()
      .map(ts =>
        (ts._2.toLong, (vocabulary(ts._2.toInt),
          ts._1.toSparse.indices
            .zip(ts._1.toSparse.values).toMap)))

    logger.info("Preparing edges for GraphX...")
    //Construct edges list for GraphX from vocabulary
    val edgeIndexes = for (i <- vocabulary.indices) yield i.toLong
    val edgeIndexesRDD = sc.parallelize(edgeIndexes)
    val edgesRDD = edgeIndexesRDD.cartesian(edgeIndexesRDD)
      .filter { case (a, b) => a < b }
      .map(pair => Edge(pair._1, pair._2, 1.0))

    logger.info("Constructing complete Graph...")
    type Vertex = (String, Map[Int, Double])
    val vertices: RDD[(VertexId, Vertex)] = verticesRDD
    val edges: RDD[Edge[Double]] = edgesRDD

    val graph = Graph(vertices, edges)

    logger.info("Training complete graph with " + edgesRDD.count() + " edges...")
    val trainedGraph = graph.mapTriplets(triplet => compareTimeSeries(triplet.dstAttr._2, triplet.srcAttr._2)).mapVertices((vID, attr) => attr._1).cache()
    logger.info("Removing low weight edges...")

    val prunedGraph = removeLowWeightEdges(trainedGraph, minWeight = 4).cache()
//  val prunedGraph = removeLowWeightEdges(trainedGraph).cache() //use mean as a minWeight threshold
    logger.info("Filtered graph with " + prunedGraph.edges.count() + " edges.")

    logger.info("Getting largest connected component...")
    logger.info("Start: " + Calendar.getInstance().getTime)
    val largestCC = getLargestConnectedComponent(prunedGraph)
    logger.info("Stop: " + Calendar.getInstance().getTime)

    logger.info("Saving graph...")
    saveGraph(largestCC, PATH_OUTPUT + "graph.gexf")
  }
}
