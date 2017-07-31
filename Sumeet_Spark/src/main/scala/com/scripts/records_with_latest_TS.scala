package com.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

case class Person (Id:Int,Name: String,value:Int)

object records_with_latest_TS {
    val sc = SparkSession.builder
    .master("local")
    .appName("records_with_latest_TS")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .config("spark.cassandra.connection.host", "localhost")
    .enableHiveSupport()
    .getOrCreate()
 import sc.implicits._
 def main(args: Array[String]) {
    val test_df = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "UNIVOCITY")
      .load("C:\\Users\\sumeet.agrawal\\Desktop\\Book1.csv").toDF()
      
      test_df.show()
      test_df.printSchema()
     val test1_df = test_df.withColumn("Id", test_df.col("Id").cast(DataTypes.IntegerType))
                            .withColumn("value", test_df.col("value").cast(DataTypes.IntegerType))
                            
   

     
      test1_df.show()
      test1_df.printSchema()
      
      
      
  val a = test1_df.as[Person].groupByKey(_.Id).reduceGroups((x, y) => if (x.value > y.value) x else y)
  
      
}
}