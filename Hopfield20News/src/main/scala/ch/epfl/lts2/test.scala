package ch.epfl.lts2

import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

/**
  * Created by volodymyrmiz on 10.10.16.
  */
object Test {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))
    /*
    Create Spark Session and define Spark Context
     */
    val spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config("spark.sql.warehouse.dir", "../")
      .getOrCreate()

    val sc = spark.sparkContext
  }
}
