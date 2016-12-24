package ch.epfl.lts2

import org.apache.spark.sql.{Row, SparkSession}
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._

import scala.reflect.io.Path

/**
  * Created by volodymyrmiz on 16.10.16.
  */
object TransposeTS {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))
    /*
    Create Spark Session and define Spark Context
     */
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Transpose column time-series")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "10g")
      .getOrCreate()

    val sc = spark.sparkContext

    println("Transposing dataset...")
    val tsRDD = sc.textFile(PATH_OUTPUT + "denseTS/text*.txt")
    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr)).cache()
//    val trRDD = sc.parallelize(rowRDD.map(_.toSeq).collect.toSeq.transpose)
    val trRDD = rowRDD.map(_.toSeq)
      .zipWithIndex()
      .map{case(seq, index) => seq.map((_, index))}
      .map(_.zipWithIndex)
      .flatMap(p=>p)
      .groupBy(_._2)
      .map(pair => (pair._1, pair._2.map(_._1).toList.sortBy(_._2).map(_._1)))
      .sortBy(_._1)
      .map(_._2)

    Path(PATH_OUTPUT + "trRDD").deleteRecursively()
    trRDD.zipWithIndex().saveAsObjectFile(PATH_OUTPUT + "trRDD")
  }
}
