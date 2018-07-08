package com.compare
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class CompareUtil extends Serializable {

  def calculate_input_variance(input_df: DataFrame, Output_df: DataFrame): DataFrame = {
    if (input_df.schema != Output_df.schema) {
      println("Input and target dataframe schema does not match")
      sys.exit(-1)
    }
    //Input dataframe record count
    val input_cnt = input_df.count()

    //Input Records Not presenr in target or with Variance in Target
    val input_var = input_df.except(Output_df)

    //Input Records exactly present as in Target
    val input_int = input_df.intersect(Output_df)
    //Count of Input records exactly present in Target
    val input_intersect_cnt = input_int.count()

    //println(s"Number of records of input exactly matching with target $input_intersect_cnt")
    //Count of Input variance records from Target(Not present/Present with variance)
    val input_variance_count = input_var.count()

    //Percentage of records from input and Target which shows 0 variance
    val intersect_percent = (input_intersect_cnt * 100.0f) / input_cnt
    println(s"$intersect_percent percent of rows are same in input and target")

    //Percentage of records from input to Target which shows variance
    val except_percent = (input_variance_count * 100.0f) / input_cnt
    println(s"Variance Total percentage is $except_percent")

    //Dropping hash columns to caluclate new records and records with Variance
    val input_var_WOHash = input_var.drop("hash")
    val target_df_WOHash = Output_df.drop("hash")

    //Count of records present in Input and Target but with variance in Values
    val intersect_records_count_with_values_variance = input_var_WOHash.intersect(target_df_WOHash).count
    //println(s"Number of records present in target from input but different values $intersect_records_count_with_values_variance")

    //Count of records present in Input and Target but with variance in Values in percentage
    val variance_in_values_percent = (intersect_records_count_with_values_variance * 100.0f) / input_variance_count
    println(s"Variance percentage if records are present in target $variance_in_values_percent")

    //Count of records from Input which are not present in Target in percentage
    var input_rows_not_present_percent = ((input_variance_count - intersect_records_count_with_values_variance) * 100.0f) / input_variance_count
    println(s"Variance percentage when the input records are not present in target $input_rows_not_present_percent")
    input_var
  }

  def zipWithIndex[U](rdd: RDD[U]) = rdd.zipWithIndex().map { case (row, idx) => (idx, row) }

  def Compare_rows(row1: Row, row2: Row, primaryKey : Seq[String]): (Boolean, String) = {
    val r1 = row1.get(1)
    var r2 = row2.get(2)
    val ret = (r1.toString.concat(r2.toString))
    var idx = 0
    val tol = 0.1
    val length_row = row1.length
    while (idx < length_row) {
      if (row1.isNullAt(idx) != row2.isNullAt(idx))
        println(s"there is a null value on column ${row1.schema.fieldNames(idx)}")

      if (!row1.isNullAt(idx)) {
        val value_input = row1.get(idx)
        val value_target = row2.get(idx)
        val rowstr = row1.mkString(",")
        value_input match {
          case b1: Array[Byte] =>
            if (!java.util.Arrays.equals(b1, value_target.asInstanceOf[Array[Byte]])) return (false, s"$b1 is not equal to ${value_target.asInstanceOf[Array[Byte]]} on column ${row1.schema.fieldNames(idx)} (column index $idx)")

          case f1: Float =>
            if (java.lang.Float.isNaN(f1) != java.lang.Float.isNaN(value_target.asInstanceOf[Float])) return (false, s"null value on column ${row1.schema.fieldNames(idx)} (column index $idx)")
            if (Math.abs(f1 - value_target.asInstanceOf[Float]) > tol) return (false, s"$f1 is not equal to ${value_target.asInstanceOf[Float]} on column ${row1.schema.fieldNames(idx)} (column index $idx)")

          case d1: Double =>
            if (java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(value_target.asInstanceOf[Double])) return (false, s"null value on column ${row1.schema.fieldNames(idx)} (column index $idx)")
            if (Math.abs(d1 - value_target.asInstanceOf[Double]) > tol) return (false, s"$d1 is not equal to ${value_target.asInstanceOf[Double]} at $tol tolerance on column ${row1.schema.fieldNames(idx)} (column index $idx)")

          case d1: java.math.BigDecimal =>
            if (d1.compareTo(value_target.asInstanceOf[java.math.BigDecimal]) != 0) return (false, s"$d1 is not equal to ${value_target.asInstanceOf[java.math.BigDecimal]} on column ${row1.schema.fieldNames(idx)} (column index $idx)")

          case _ =>
            if (value_input != value_target) return (false,s""" Row  "${row1.mkString(",")}" does not match $value_input is not equal to $value_target on column ${row1.schema.fieldNames(idx)}  (column index $idx)""")
        }
      }
      idx += 1
    }
    return (true, "Row completely matched")
  }
}
