package com.scripts

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class SampleRddTest extends FunSuite with SharedSparkContext {

  test("Testing RDD transformations using a shared Spark Context") {
    val input = List("Testing", "RDD transformations", "using a shared", "Spark Context")
    val expected = Array(Array("Testing"), Array("RDD", "transformations"), Array("using", "a", "shared"),
      Array("Spark", "Context"))
    val transformed = SampleRdd.tokenize(sc.parallelize(input))
    assert(transformed === expected)
  }

}