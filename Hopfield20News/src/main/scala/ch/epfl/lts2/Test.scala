package ch.epfl.lts2

import breeze.linalg.Matrix
import org.apache.spark.sql.{Row, SparkSession}
import ch.epfl.lts2.Utils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by volodymyrmiz on 10.10.16.
  */
object Test {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Transpose column time-series")
      .config("spark.sql.warehouse.dir", "../")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.executor.cores", "1")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    val sc = spark.sparkContext

    println("Transposing dataset...")

    val tsRDD = sc.objectFile[ArrayBuffer[String]]("./data/trRDD").collect()
    val tsRDD1 = sc.objectFile[ArrayBuffer[String]]("./data/trRDDdistr").collect()

    println(tsRDD(1000).toList.map(_.toDouble).filter(_ != 0.0))
    println(tsRDD1(1000).toList.map(_.toDouble).filter(_ != 0.0))

//
//    println("Slitting...")
//    val rowRDD = tsRDD.map(_.split(",")).map(attr => Row.fromSeq(attr)).cache()
//    println(rowRDD.collect().mkString("\n"))
//    println("Transposing...")
//    val trRDD = sc.parallelize(rowRDD.map(_.toSeq).collect.toSeq.transpose)
//    println(trRDD.collect().mkString("\n"))
//
//
//    val transposed = rowRDD.map(_.toSeq).flatMap(row => (row.map(col => (col, row.indexOf(col))))) // flatMap by keeping the column position
//      .map(v => (v._2, v._1)) // key by column position
//      .groupByKey().sortByKey()   // regroup on column position, thus all elements from the first column will be in the first row
//      .map(_._2)              // discard the key, keep only value
//
//    println(transposed.collect().mkString("\n"))

    //    val v1 = Vectors.dense(1,0,1,1,1).toSparse
    //    val v2 = Vectors.dense(1,1,1,0,0).toSparse
    //    println(v1.indices.mkString(","))
    //    println(Vectors.sqdist(v1, v2))
    //
    //    val s1 = Seq(1,2,3)
    //    val s2 = Seq(2,3,4)
    //    println(s1.intersect(s2).length)

    //    println(List.range(1, 10))

    //    println(StopWordsRemover.loadDefaultStopWords("english").mkString(", "))

    //    import scala.math.log
    //    println(log(16)/log(2))

    //    val m = Map(1->1, 1->1, 3->1)
    //    println(m.keys.toList)

/////////////////////////////////////////////////
//    val list1 = List(1,2,3)
//    val list2 = List(10,20,30)
//
//    val m1 = list1.zip(list2).toMap
//    println(m1)
//
//    val l1 = List(4,5,6)
//    val l2 = List(100, 300, 400)
//    val m2 = l1.zip(l2).toMap
//    println(m2)
//
//    val commonKeys = m2.keySet.intersect(m1.keySet)
//    println(commonKeys)
//
//    var weight = 0
//    for(key <- commonKeys){
//      weight += (m1.get(key).get + m2.get(key).get)/2
//    }
//    if (commonKeys.size == 0) println(0)
//    else println(weight/commonKeys.size)
    ///////////////////////////////////////////////


  }
}
