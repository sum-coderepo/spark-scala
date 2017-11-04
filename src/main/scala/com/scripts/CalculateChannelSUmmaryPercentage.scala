package com.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions

object test {
    val sc = SparkSession.builder
    .master("local")
    .appName("Secondary_sort")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
    
  def main(args: Array[String])={
    println("Hello World")
    
       val df_subcat_channel = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "UNIVOCITY")
      .load("C:\\Users\\sumeet.agrawal\\Desktop\\ddd\\file1.csv").toDF()
      
      df_subcat_channel.show()
      
       val df_client = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "UNIVOCITY")
      .load("C:\\Users\\sumeet.agrawal\\Desktop\\ddd\\part-r-00000-b832513f-397f-4129-a3dc-4be8f1a62ee2.csv").toDF()
      
      df_client.show()
      
      val join = df_subcat_channel.join(df_client, df_subcat_channel("Subcat") === df_client("SRA2"))
      join.show()
      
      val join2 = join.withColumn("VAL", (join("VAL")).cast(DataTypes.FloatType))
      join2.show()
     
     val join1 = join2.filter(join2("Channel Summary")==="CLINICS").groupBy("PGN", "DATE").sum("val")
   
     println(join1.count())
     join1.show()
     
     val join3 = join1.join(join2, join2("PGN")===join1("PGN") && join2("DATE")===join1("DATE"))
     val join4 = join3.withColumn("Percentage", (join3("val")/join3("sum(val)")).cast(DataTypes.FloatType))
     
     join4.show(1000)
  }
}