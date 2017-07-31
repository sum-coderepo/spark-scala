package com.conf


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession


/**
  * To configure the Spark job
  *
  */

trait Conf {
    val sc : org.apache.spark.sql.SparkSession
    
}
//test

/**
  * allowMultipleContexts is set on true. But ideally every processing that needs a Spark context should use the same ETLConf object.
  * to use it, put
  * import com.sky.conf.ETLConf._
  * wherever needs a Spark context
  */
object ETLConf extends Conf{
  
   val sc = SparkSession.builder
    .master("local")
    .appName("spark jenssen job")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()

    // Logging
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Enable snappy compression for Avro
    sc.conf.set("spark.sql.avro.compression.codec", "snappy")

    val log = LoggerFactory.getLogger(this.getClass.getName)
}
