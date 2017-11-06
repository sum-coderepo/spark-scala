package com.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

case class Record(
  foo: Double, target: Double, x1: Double, x2: Double, x3: Double)
  
  case class Person(name: String, age: Int)
object LabelledPointExamples {

  def main(args: Array[String]) = {
    /* val conf = new SparkConf().setAppName("Big Apple").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc) */

    val sc = SparkSession.builder
      .master("local")
      .appName("kmeans")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .enableHiveSupport()
      .getOrCreate()

    import sc.implicits._
    import scala.util.Random.{ setSeed, nextDouble }
    setSeed(1)
    
    val input = sc.sparkContext.parallelize(Seq(Person("a", 1), Person("b", 2)))
    val input_df = sc.sqlContext.createDataFrame(input)
    
    input_df.show()

    val rows = sc.sparkContext.parallelize(
      (1 to 10).map(_ => Record(
        nextDouble, nextDouble, nextDouble, nextDouble, nextDouble)))

    val df = sc.sqlContext.createDataFrame(rows)
    println("Hello")
    df.show()
    df.registerTempTable("df")

    sc.sqlContext.sql("""
  SELECT ROUND(foo, 2) foo,
         ROUND(target, 2) target,
         ROUND(x1, 2) x1,
         ROUND(x2, 2) x2,
         ROUND(x2, 2) x3 
  FROM df""").show
  }
}