package com.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.Row

object sample {
   val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:\\Users\\suagrawa\\workspace\\SCE_ENGG")
   // .enableHiveSupport()
    .getOrCreate()
 def main(args: Array[String]) = {

  //   println(a.filecheck("C:\\Users\\sumeet.agrawal\\Downloads\\eclipse"))
    val Claim1 = StructType(Seq(StructField("pid", StringType, true),StructField("diag1", StringType, true),StructField("diag2", StringType, true), StructField("allowed", IntegerType, true), StructField("allowed1", IntegerType, true)))
val claimsData1 = Seq(("PID1", "diag1", "diag2", 100, 200), ("PID1", "diag2", "diag3", 300, 600), ("PID1", "diag1", "diag5", 340, 680), ("PID2", "diag3", "diag4", 245, 490), ("PID2", "diag2", "diag1", 124, 248))

val claimRDD1 = sc.sparkContext.parallelize(claimsData1)
val claimRDDRow1 = claimRDD1.map(p => Row(p._1, p._2, p._3, p._4, p._5))
val claimRDD2DF1 = sc.sqlContext.createDataFrame(claimRDDRow1, Claim1)

   println(SizeEstimator.estimate(claimRDD2DF1))
val l = List("allowed", "allowed1")
val exprs = l.map((_ -> "sum")).toMap
claimRDD2DF1.groupBy("pid").agg(exprs) show false
val exprs1 = Map("allowed" -> "sum", "allowed1" -> "avg")

claimRDD2DF1.groupBy("pid").agg(exprs1) show false
   }
}