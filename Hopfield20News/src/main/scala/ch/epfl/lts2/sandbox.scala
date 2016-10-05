package ch.epfl.lts2

import java.sql.Struct

import ch.epfl.lts2.Utils._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io._

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

    import spark.implicits._

    val sc = spark.sparkContext

    val doc = List("our", "text", "template", "aimed", "to", "extract", "time", "series", "from", "text")

    val windowedDoc = doc.sliding(3, 3)
    //    Seq("another", "text", "template", "in", "order", "to", "extract", "series", "from", "text")

    val vocabulary = List("text", "template", "aimed", "extract", "time", "series", "another", "order")

    val vocabLength = vocabulary.length

    val pw = new PrintWriter(new File("test.txt"))
    for (window <- windowedDoc) {
      val indexes = vocabulary.filter(window.contains(_)).map(word => vocabulary.indexOf(word))
      val vector = Vectors.sparse(vocabLength, indexes.toArray, Array.fill(indexes.length)(1)).toDense.toString()
      pw.write(vector.substring(1, vector.length - 1) + "\n")
    }
    pw.close()

    /** *
      * DataFrame of time series
      */
    val fields = vocabulary.map(StructField(_, StringType, nullable = true))
    val schema = StructType(fields)

    val tsRDD = spark.sparkContext.textFile("./test.txt")

    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr))
    val tsDF = spark.createDataFrame(rowRDD, schema)
    tsDF.show()
  }
}
