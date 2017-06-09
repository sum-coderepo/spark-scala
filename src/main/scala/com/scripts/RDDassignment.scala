package com.scripts

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
  

object RDDAssignment {
 def main (args: Array[String]) {
 import PageCounter._  


  val conf = new SparkConf().setAppName("Big Apple").setMaster("local[4]")
   val sc = new SparkContext(conf)

  // Page Counts RDD
  val pageCountsRdd = sc.textFile("C:\\Users\\sumeet.agrawal\\Downloads/pagecounts-20151201-220000.gz")

  // 10 records
  extractHeadByCount(pageCountsRdd, 10) foreach println

  //Total Records
  println(s"Total records = ${pageCountsRdd.count}")

  //English Pages
  val englishPages = filterByString(pageCountsRdd, "/en/")

  // English Pages Count
  println(s" English pages = ${englishPages.count()}")

  // Requested > 200K
   val combineByKeyRDD = reduceByKeyWithFunction(pageCountsRdd, (a: Int, b: Int) => a + b)
   val pageCountGreaterThan200K = filterByFunction(combineByKeyRDD, (x => (x._2 > 200000)))


  println(s" Pages Requested>200k =${pageCountGreaterThan200K.count()} ")

  sc.stop()
}
  }
  

object PageCounter {
  def extractHeadByCount(rdd: RDD[String], n: Int) = rdd.take(n)

  def filterByString(rdd: RDD[String], s: String) = rdd.filter(line => line.contains(s))

  def filterByFunction(rdd: RDD[(String, Int)], fnc: ((String, Int)) => Boolean) = rdd.filter(fnc)


  def reduceByKeyWithFunction(rdd: RDD[String], fnc: (Int, Int) => Int) = {
    val splitRDD = rdd.map(_.split(" "))
    val combineByKeyRDD = splitRDD.map(x => (x(1), x(2).toInt))
    combineByKeyRDD.reduceByKey(fnc)
  }
}