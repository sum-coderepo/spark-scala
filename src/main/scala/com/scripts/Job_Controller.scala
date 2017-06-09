package com.scripts

import java.util.Calendar
import java.util.Date
//import scala.util.Properties
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.util.ArrayList
import org.apache.spark.rdd.RDD

object Secondary_sort {

  val sc = SparkSession.builder
    .master("local")
    .appName("Secondary_sort")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]) = {

    val start_time = getTime()
    println(getTime())

    val JobProperties = new Properties()
    JobProperties.setProperty("JobName", "FileNameRead")
    JobProperties.setProperty("BatchId", "101")
    JobProperties.setProperty("Status", "Run")
    JobProperties.setProperty("ST_Time", s"$start_time")

    println("The current script running is " + JobProperties.getProperty("JobName"))

    val df_real_estate = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "UNIVOCITY")
      .load("C:\\Users\\sumeet.agrawal\\Desktop\\Sacramentorealestatetransactions.csv").toDF()

    df_real_estate.show()
    val End_Time = getTime()
    println(getTime())
    JobProperties.setProperty("ED_Time", s"$End_Time")
    val map = JobProperties.asScala.toMap
    CreateDataframe(map)
     println(map)

  }
  def getTime(): String = {

    Calendar.getInstance().getTime().toString()

  }

  def CreateDataframe(map: Map[String, String]): DataFrame = {
    val schemaString = map.keys.mkString(",")
    println(schemaString)
    val schema = StructType(schemaString.split(",").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    val rdd = map.values.mkString(",").split(",")
    println(rdd.length)
    val rdd2 : RDD[Row]= sc.sparkContext.makeRDD(Seq(Row(rdd(0).toString(),rdd(1).toString(),rdd(2).toString(),rdd(3).toString(),rdd(4).toString())))
    var datafr = sc.sqlContext.createDataFrame(rdd2, schema)
    println(datafr.schema.length)
    datafr.show
    datafr

  }

}