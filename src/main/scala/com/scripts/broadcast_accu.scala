package com.scripts

import org.apache.spark.sql.SparkSession

object broadcast_accu   {
  val sc = SparkSession.builder

    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
 def main(args: Array[String]) = { 

val broadcastVar = sc.sparkContext.broadcast(Array(1, 2, 3))
  broadcastVar.value


  val accum = sc.sparkContext.accumulator(0)

  val anRDD = sc.sparkContext.parallelize(1 to 10)

  val transformedRDD = anRDD map (x => {
    accum += 1
    println(s" in tranformation $accum")
    x + accum.value
  })

  //transformedRDD foreach println

  println(accum)

  println(anRDD.reduce((x, y) => {
    accum += 1
    println(s" in action $accum")
    x + y + accum.value
  }))

  println(accum.value)
}
  
}