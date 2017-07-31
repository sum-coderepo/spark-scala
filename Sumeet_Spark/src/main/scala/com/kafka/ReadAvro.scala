package com.kafka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.common.Util
import org.apache.avro.io.DatumReader
import org.apache.avro.io._
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import org.apache.avro.file.DataFileReader

object ReadAvro {
    val sc = SparkSession.builder
    .master("local")
    .appName("Write parquet")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    //.enableHiveSupport()
    .getOrCreate()
  import sc.sqlContext.implicits._
  
 def main(args: Array[String]) {
      
      import com.databricks.spark.avro._

val sqlContext = sc.sqlContext

import sqlContext.implicits._

var plane_data_df = sqlContext.read.format("com.databricks.spark.avro")
                   .load("hdfs://localhost:9000/user/sumeet/Decomposed/plane_data/part-m-00000.avro");

plane_data_df = Util.TrimAllColumns(plane_data_df)
plane_data_df = plane_data_df.select("*").distinct()
plane_data_df = plane_data_df.filter(col("tailnum").isNotNull)
plane_data_df.show()

//plane_data_df.coalesce(1).write.parquet("hdfs://localhost:9000/user/sumeet/data/modelled/plane_data_parquet")
      
var carriers_df = sqlContext.read.format("com.databricks.spark.avro")
                   .load("hdfs://localhost:9000/user/sumeet/Decomposed/carriers/part-m-00000.avro");

carriers_df = Util.TrimAllColumns(carriers_df)
carriers_df = carriers_df.select("*").distinct()
carriers_df = carriers_df.filter(col("code").isNotNull)
carriers_df.show()

//carriers_df.coalesce(1).write.parquet("hdfs://localhost:9000/user/sumeet/data/modelled/carriers_parquet")

var airports_df = sqlContext.read.format("com.databricks.spark.avro")
                   .load("hdfs://localhost:9000/user/sumeet/Decomposed/airports/part-m-00000.avro");

airports_df = Util.TrimAllColumns(airports_df)
airports_df = airports_df.select("*").distinct()
airports_df = airports_df.filter(col("iata").isNotNull)
airports_df.show()

//airports_df.coalesce(1).write.parquet("hdfs://localhost:9000/user/sumeet/data/modelled/airports_parquet")

var OTP_2007_df = sqlContext.read.format("com.databricks.spark.avro")
                   .load("hdfs://localhost:9000/user/sumeet/Decomposed/OTP_2007/part-m-00000.avro");

var OTP_2008_df = sqlContext.read.format("com.databricks.spark.avro")
                   .load("hdfs://localhost:9000/user/sumeet/Decomposed/OTP_2008/part-m-00000.avro");

var OTP_DF = Util.unionByName(OTP_2007_df, OTP_2008_df)

OTP_DF = Util.TrimAllColumns(OTP_DF)
OTP_DF = OTP_DF.select("*").distinct()
OTP_DF = OTP_DF.filter(col("year").isNotNull)
OTP_DF.show()
//OTP_DF.coalesce(1).write.parquet("hdfs://localhost:9000/user/sumeet/data/modelled/OTP_paraquet")



  }
}