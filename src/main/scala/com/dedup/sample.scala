package com.dedup

import java.io.File

import com.scripts.class_validation.sc
import org.apache.spark.sql.functions.{asc, desc}

object sample {

  def main(args: Array[String]) = {
    /*var input = sc.sqlContext.createDataFrame(Seq(
      ("A", 101, "Sumeet", "Maths", 98, 18, "Cricket"),
      ("A", 101, "Sumeet", "English", 95, 18, "Cricket"),
      ("A", 101, "Sumeet", "History", 96, 18, "Cricket"),
      ("A", 102, "Ajay", "Maths", 87, 19, "Football"),
      ("A", 102, "Ajay", "English", 86, 19, "Football"),
      ("A", 102, "Ajay", "History", 86, 19,"Football"),
      ("A", 103, "Rahul", "Maths", 73, 19, "Music"),
      ("A", 103, "Rahul", "English", 72, 19, "Music"),
      ("A", 103, "Rahul", "History", 78, 19, "Music"),
      ("B", 101, "Leonardo", "Economics", 65, 20, "Sleeping"),
      ("B", 101, "Leonardo", "Martial Arts", 61, 20, "Sleeping"),
      ("B", 101, "Leonardo", "Mathematics", 68, 20, "Sleeping"),
      ("B", 102, "Mark", "Economics", 88, 19, "Running"),
      ("B", 102, "Mark", "Martial Arts", 89, 19, "Running"),
      ("B", 102, "Mark", "Mathematics", 82, 19, "Running")
    )).toDF("class", "id", "name", "subject" ,"score","age", "hobby")
    input.show()*/
    val x = new SimpleDateBasedPartitionEstimator("file:///C:/Windows/debug","yyyy-MM-dd",sc.sparkContext)
    x.estimate.toString.foreach(s => println(s))
  }

}
