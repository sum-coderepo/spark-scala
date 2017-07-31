package com.scripts

import org.apache.spark.sql.SparkSession

object partioning {
    def main(args: Array[String]) {
  
    val sc = SparkSession.builder
    .master("local")
    .appName("records_with_latest_TS")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
 }
}