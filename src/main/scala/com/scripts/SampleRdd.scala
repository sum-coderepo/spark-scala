package com.scripts
import org.apache.spark.rdd.RDD
object SampleRdd {

  def tokenize(aL: RDD[String]) = {
    aL.map(x => x.split(' ')).collect()
  }

}
