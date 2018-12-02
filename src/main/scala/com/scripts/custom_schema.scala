package com.scripts

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession

object custom_schema {
  def main(args: Array[String]) {
  
    val sc = SparkSession.builder
    .master("local")
    .appName("records_with_latest_TS")
    .config("spark.sql.warehouse.dir", "file:///D:/spark_hive")
    //.enableHiveSupport()
    .getOrCreate()
 import sc.implicits._
    
  val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true)))

  println(customSchema)
  val newschema = customSchema.add(StructField("NewField", StringType, nullable = true))
    println(newschema)
  sys.exit(0)
      
    /*val test_df = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("parserLib", "UNIVOCITY")
      .schema(customSchema)
      .load("C:\\Users\\sumeet.agrawal\\Desktop\\Book1.csv").toDF()
      
      test_df.show() */
      
       val emp = sc.sparkContext.parallelize(Seq((1, "revanth", 10), (2, "dravid", 20), (3, "kiran", 30), (4, "nanda", 35), (5, "kishore", 30)))

    // Create dept RDD
    val dept = sc.sparkContext.parallelize(Seq(("hadoop", 10), ("spark", 20), ("hive", 30), ("sqoop", 40)))
        dept.toDF().show()
    // Establishing that the third field is to be considered as the Key for the emp RDD
    val manipulated_emp = emp.keyBy(t => t._3)

    // Establishing that the second field need to be considered as the Key for dept RDD
    val manipulated_dept = dept.keyBy(t => t._2)
    manipulated_dept.foreach(x => println(x))
}
}