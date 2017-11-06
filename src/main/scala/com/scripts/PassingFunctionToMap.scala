package com.scripts

import scala.collection.mutable.MutableList
import org.apache.spark.sql.SparkSession

object PassingFunctionToMap {
  def main(args: Array[String]) = {

    val sc = SparkSession.builder
      .master("local")
      .appName("kmeans")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
      .enableHiveSupport()
      .getOrCreate()
      
      import sc.implicits._
      
    val str = "gwdwhqkdhqjldj"
    //countchar(str)
    //print("completed")

    val x = Seq(("a", 36), ("b", 33), ("c", 40), ("a", 38), ("c", 39)).toDS()
    val g = x.groupByKey(_._1)
    val z = g.mapGroups{case(k, iter) => (k, iter.map(x => x._2).toArray)}
    
    z.show()

  }

  def countchar(str: String): MutableList[String] = {
    val list = scala.collection.mutable.MutableList[String]()
    val lst = scala.collection.mutable.MutableList[String]()
    for (i <- str) {
      list += i.toString()
    }
    println(list)
    val s = list.map(x => (x, 1)).groupBy(_._1)
    val list1 = list.map(x => (x.split(" ")(0), 1)).toList
    val list2 = list1.groupBy(_._1).mapValues(x => x.map(_._2).reduce(_ + _))
    val list3 = list2.map(x => mul(x))

    for (x <- list2.keys) {
      var s = ""
      s = list2(x).toString().+(x)
      lst += s
    }

    println(list3)
    println(list2)
    list
  }

  var mul = (a: (String, Int)) => a._2.+(a._1)

}