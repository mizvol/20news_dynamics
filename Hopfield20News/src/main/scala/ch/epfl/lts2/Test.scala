package ch.epfl.lts2

import breeze.linalg.Matrix
import org.apache.spark.sql.{Row, SparkSession}
import ch.epfl.lts2.Utils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.graphx.GraphLoader

/**
  * Created by volodymyrmiz on 10.10.16.
  */
object Test {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Test")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.executor.cores", "1")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    val sc = spark.sparkContext
  }
}
