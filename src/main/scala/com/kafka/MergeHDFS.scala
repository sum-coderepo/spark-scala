package com.kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds

object MergeHDFS {
  
       val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
  def main(args:Array[String]) = {
         
      val test = sc.sparkContext.textFile("hdfs://localhost:9000/user/sumeet/data/raw/Airports/17-07-28/*", 10)   
      
      test.coalesce(1).saveAsTextFile("hdfs://localhost:9000/user/sumeet/data/raw/Airports/airports.csv")
         
       
       }
  
}