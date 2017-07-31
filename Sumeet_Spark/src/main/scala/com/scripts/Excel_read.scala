package com.scripts

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.joda.time.DateTime

object Excel_read {
  val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
  import sc.sqlContext.implicits._
  def main(args: Array[String]) = {

    val df_dollars = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "UNIVOCITY")
      .load("C:/Users/sumeet.agrawal/Desktop/Files/CDS.ZPHX.DOLLARS.CLI132.R45.W42.csv").toDF()

    val df_productList = sc.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "UNIVOCITY")
      .load("C:/Users/sumeet.agrawal/Desktop/Files/Immunology-Product_List_2843.csv").toDF().select("Product Group ID", "Product Name")
      
    var a = df_dollars.select("Dt").distinct().collect()(0).getString(0).toString()
    println(a)

    println(df_dollars.columns.length)
    val list = df_dollars.columns.map(x => x.toString()).toList
    println(list)
    var dm = List[String]()
    var int = 0
    for ((s, i) <- list.zipWithIndex) {
      if (i < 10) {
        dm = s :: dm
      } else {
        val format = new java.text.SimpleDateFormat("yyMMdd")
        var date1 = format.parse(a)
        val dateTime = new DateTime(date1);
        //   println("dateTime :  " + dateTime)
        dm = a :: dm
        val newdt = dateTime.minusDays(7)
        //   println("newdt:  " + newdt.toString())
        a = newdt.toString().slice(2, 4).concat(newdt.toString().slice(5, 7)).concat(newdt.toString().slice(8, 10))
      }
    }
    val newlst = dm.reverse.toSeq
    println(newlst)

    val df_Dollars_Renamed = df_dollars.toDF(newlst: _*)
    df_Dollars_Renamed.show()

    val df_Dollars_Transpose = toLong(df_Dollars_Renamed, newlst.slice(0, 10))
    df_Dollars_Transpose.show()
    
    println("Join Started")
    
   val final_df_Joined =  df_Dollars_Transpose
    .join(df_productList, df_Dollars_Transpose("PGN") === df_productList("Product Group ID"), "left_outer")
    
    final_df_Joined.show(300)
    
    
    println("Finished")
  }

  def toLong(df: DataFrame, by: Seq[String]): DataFrame = {
    val (cols, types) = df.dtypes.filter { case (c, _) => !by.contains(c) }.unzip
    require(types.distinct.size == 1)

    val kvs = explode(array(
      cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*))

    val byExprs = by.map(col(_))

    
    df.select(byExprs :+ kvs.alias("_kvs"): _*)
      .select(byExprs ++ Seq(kvs.getField("_kvs.key"),kvs.getField("_kvs.val")): _*)
  }
}