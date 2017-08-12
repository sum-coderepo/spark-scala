package com.scripts

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object CustomPartitionerDemo {
    val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
  import sc.sqlContext.implicits._
  
 def main(args: Array[String]) {

   val inputFile = sc.sparkContext.textFile("C:\\Users\\sumeet.agrawal\\Desktop\\1.txt")
  
  //create paired RDD 
  val pairedData = inputFile.flatMap ( x => x.split(" ") ).map(x => (x,1))
  
  //Define custom pertitioner for paired RDD
  val partitionedData=pairedData.partitionBy(new MyCustomerPartitioner(2))
   
  //verify result using mapPartitionWithIndex
  val finalOut = partitionedData.mapPartitionsWithIndex{(partitionIndex ,dataIterator) =>dataIterator.map(dataInfo => (dataInfo +" is located in  " + partitionIndex +" partition."))}
  
  //Save Output in HDFS
  finalOut.saveAsTextFile("C:\\Users\\sumeet.agrawal\\Desktop\\partitionOutput")

 }
}
//
class MyCustomerPartitioner(numParts: Int) extends Partitioner {
 override def numPartitions: Int = numParts
 
 override def getPartition(key: Any): Int = 
 {
     val out = toInt(key.toString)
       out
 }

 override def equals(other: Any): Boolean = other match 
 {
 case dnp: MyCustomerPartitioner =>
 dnp.numPartitions == numPartitions
 case _ =>
 false 
 }
 
 def toInt(s: String): Int = 
 {
  try {
   s.toInt
   0
  } catch {
  case e: Exception => 1
  }
 }
}
