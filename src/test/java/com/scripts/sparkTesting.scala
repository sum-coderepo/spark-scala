package com.scripts

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.conf.ETLConf

class SampleTest extends FunSuite with SharedSparkContext {
  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
  
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }
}