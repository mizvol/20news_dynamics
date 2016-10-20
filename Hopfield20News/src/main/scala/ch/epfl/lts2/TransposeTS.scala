package ch.epfl.lts2

import org.apache.spark.sql.{Row, SparkSession}
import ch.epfl.lts2.Utils._

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
    val tsRDD = sc.textFile("./denseTs/text*.txt")
    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr)).cache()
    val trRDD = sc.parallelize(rowRDD.map(_.toSeq).collect.toSeq.transpose)

    trRDD.zipWithIndex().saveAsObjectFile("./data/trRDD")
  }
}
