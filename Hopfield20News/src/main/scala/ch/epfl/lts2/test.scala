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

//    val v1 = Vectors.dense(1,0,1,1,1).toSparse
//    val v2 = Vectors.dense(1,1,1,0,0).toSparse
//    println(v1.indices.mkString(","))
//    println(Vectors.sqdist(v1, v2))
//
//    val s1 = Seq(1,2,3)
//    val s2 = Seq(2,3,4)
//    println(s1.intersect(s2).length)


    println(List.range(1, 10))

  }
}
