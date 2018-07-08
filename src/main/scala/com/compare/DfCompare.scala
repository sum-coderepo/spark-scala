package com.compare

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{hash, col, expr}
import com.compare.CompareUtil
object DfCompare {
  val sc = SparkSession.builder
    .master("local")
    .appName("dfcompare")
    .enableHiveSupport()
    .getOrCreate()
  def main(args: Array[String]) = {
    val testobj = new TestComapreDf
    val comUtil = new CompareUtil

    // Created input and Target Data frames
    val input_df = testobj.input
    var target_df = testobj.target

    // Primary keys and Primary keys with Hash column
    val comp_cols = Seq("name","id","Subject")
    val comp_cols_hash = comp_cols :+ "hash"

    //Added hash columns in input and target
    val hash_inputdf = input_df.withColumn("hash", hash(input_df.columns.map(col):_*))
    val hash_targettdf = target_df.withColumn("hash", hash(input_df.columns.map(col):_*))

    //Selected primary keys and hash columns from input and target
    val select_input_hash = hash_inputdf.select(comp_cols_hash.map(name => col(name)):_*)
    val select_target_hash = hash_targettdf.select(comp_cols_hash.map(name => col(name)):_*)

    //Calculate Variance records Includes new records and and records with variance
    val variance = comUtil.calculate_input_variance(select_input_hash,select_target_hash)
    variance.show()

    //Extract variant records from Target
    val variant_rows_target = target_df.join(variance.drop("hash"),comp_cols).orderBy(comp_cols.map(name => col(name)):_*)

    variant_rows_target.show(10)
    //Converted Target Variant records into RDD
    val variant_rows_target_rdd = comUtil.zipWithIndex(variant_rows_target.rdd)

    //Extract variant records from Input
    val variant_rows_input = input_df.join(variant_rows_target.select(comp_cols.map(name => col(name)):_*),comp_cols).orderBy(comp_cols.map(name => col(name)):_*)

    //Converted Input Variant records into RDD
    val variant_rows_input_rdd = comUtil.zipWithIndex(variant_rows_input.rdd)
    variant_rows_input.show(10)

    variant_rows_target_rdd.join(variant_rows_input_rdd).take(10).foreach(s => println(s))
    val calc_variance_rows = variant_rows_target_rdd.join(variant_rows_input_rdd)
       .map { case (idx, (r1, r2)) => comUtil.Compare_rows(r1, r2, comp_cols)
      }
    println(calc_variance_rows.foreach(x => println(x)))
    println("Done")
    }
}
