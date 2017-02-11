import ch.epfl.lts2.Utils._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import ch.epfl.lts2.Globals._
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}

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
      .config("spark.executor.cores", "5")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    val sc = spark.sparkContext

    val trRDD = sc.objectFile[(SparseVector, Long)](PATH_OUTPUT + "trRDD").sortBy(_._2).map(_._1)

    val vocabulary = sc.objectFile[(String, Long)](PATH_OUTPUT + "vocabRDD").sortBy(_._2).map(_._1).collect().toList

    println("Vocabulary length: " + vocabulary.length)
    println("Preparing vertices for GraphX...")

    val normalizer = new Normalizer()

    val verticesRDD = trRDD.map(activation => normalizer.transform(activation))
      .zipWithIndex()
      .map(ts =>
        (ts._2.toLong, (vocabulary(ts._2.toInt),
          ts._1.toSparse.indices
            .zip(ts._1.toSparse.values).toMap)))

    println("Preparing edges for GraphX...")
    //Construct edges list for GraphX from vocabulary
    val edgeIndexes = for (i <- vocabulary.indices) yield i.toLong
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
    //    val prunedGraph = removeLowWeightEdges(trainedGraph, minWeight = 2).cache()
    val prunedGraph = removeLowWeightEdges(trainedGraph).cache()
    println("Filtered graph with " + prunedGraph.edges.count() + " edges.")
    println("Removing sigletone vertices...")
    val connectedGraph = removeSingletons(prunedGraph).cache()
    println(connectedGraph.vertices.count() + " vertices remain.")

    println("Saving graph...")
    saveGraph(connectedGraph, PATH_OUTPUT + "graph.gexf")
  }
}
