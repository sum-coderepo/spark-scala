package com.dedup

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
  * An object to estimate the number of partitions that should be needed for optimal performance of most Spark jobs.
  *
  * Created by anish on 22/04/17.
  */
abstract class PartitionEstimator extends Serializable {
  def estimate: Int
}

/**
  * File path based Partition Estimator. Based on directories groups 24 hourly files into one partition
  *
  * datePattern should be a valid RegEx. It is parsed to scala.util.matching. Regex inside the function since the Regex
  * class is not Serializable
  */
class SimpleDateBasedPartitionEstimator(inputFilePath: String, datePattern: String, sparkContext: SparkContext) extends PartitionEstimator {

  override def equals(other: Any): Boolean = other match {
    case e: SimpleDateBasedPartitionEstimator =>
      e.estimate == estimate
    case _ =>
      false
  }

  lazy val estimate: Int = {
    val datePatternRegex = datePattern.r
    val dateRegex = """([0-9]{4}-[0-9]{2}-[0-9]{2})""".r
    println(datePatternRegex)
    val fs = FileSystem.get(new URI(inputFilePath), sparkContext.hadoopConfiguration)
    val m = fs.listStatus(new Path(inputFilePath)).map(_.getPath.toString.split("/").reverse(0))
    m.foreach(x => println(x))
    print(m)
    fs.listStatus(new Path(inputFilePath))
      .map(_.getPath.toString.split("/").reverse(0))
      .map {
        case dateRegex(date) => date
        case _ => println("ERROR : did not match given regex")
      }
      .distinct
      .length
  }

  override def hashCode: Int = estimate
}

/**
  * Exact number of partitions based on input files. Assumed that all input files will have a similar number of records.
  * It doesn't do any Sampling to arrive at this value.
  */
class InputSplitBasedPartitionEstimator(inputFilePath: String, sparkContext: SparkContext) extends PartitionEstimator {

  override def equals(other: Any): Boolean = other match {
    case e: InputSplitBasedPartitionEstimator =>
      e.estimate == estimate
    case _ =>
      false
  }

  lazy val estimate: Int = sparkContext
    .wholeTextFiles(inputFilePath)
    .map(_._1)
    .count()
    .toInt

  override def hashCode: Int = estimate
}