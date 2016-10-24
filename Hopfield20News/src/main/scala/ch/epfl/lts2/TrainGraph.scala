import ch.epfl.lts2.Utils._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by volodymyrmiz on 05.10.16.
  */
object TrainGraph {
  def main(Args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))
    /*
    Create Spark Session and define Spark Context
     */
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Hopfileld Filtering 20NEWS")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.executor.cores", "1")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    val sc = spark.sparkContext

//    val trRDD = sc.objectFile[(ArrayBuffer[String], Long)]("./data/trRDD").sortBy(_._2).map(_._1)
    val trRDD = sc.objectFile[(List[String], Long)]("./data/trRDD").sortBy(_._2).map(_._1)

    val vocabulary = sc.objectFile[(String, Long)]("./data/vocabRDD").sortBy(_._2).map(_._1).collect().toList

    println("Vocabulary length: " + vocabulary.length)
    println("Preparing vertices for GraphX...")
    val verticesRDD = trRDD
      .zipWithIndex()
      .map(ts =>
        (ts._2.toLong, (vocabulary(ts._2.toInt),
          Vectors.dense(ts._1.map(_.toString.toDouble).toArray).toSparse.indices
            .zip(Vectors.dense(ts._1.map(_.toString.toDouble).toArray).toSparse.values).toMap)))

    println("Preparing edges for GraphX...")
    //Construct edges list for GraphX from vocabulary
    val edgeIndexes = for (i <- 0 to vocabulary.length - 1) yield i.toLong
    val edgeIndexesRDD = sc.parallelize(edgeIndexes)
    val edgesRDD = edgeIndexesRDD.cartesian(edgeIndexesRDD)
      .filter { case (a, b) => a < b }
      .map(pair => Edge(pair._1, pair._2, 1.0))

    println("Constructing complete Graph...")
    type Vertex = (String, Map[Int, Double])
    val vertices: RDD[(VertexId, Vertex)] = verticesRDD
    val edges: RDD[Edge[Double]] = edgesRDD

    val graph = Graph(vertices, edges)

    println("Training complete graph with " + edgesRDD.count() + " edges...")
    val trainedGraph = graph.mapTriplets(triplet => compareTimeSeries(triplet.dstAttr._2, triplet.srcAttr._2)).mapVertices((vID, attr) => attr._1).cache()
    println("Removing low weight edges...")
    val prunedGraph = removeLowWeightEdges(trainedGraph, minWeight = 3.0).cache()
    println("Filtered graph with " + prunedGraph.edges.count() + " edges.")
    println("Removing sigletone vertices...")
    val connectedGraph = removeSingletons(prunedGraph).cache()
    println(connectedGraph.vertices.count() + " vertices remain.")

    println("Saving graph...")
    saveGraph(connectedGraph, "oneCategory.gexf")
  }
}
