package com.scripts

import scala.collection.mutable.MutableList

object PassingFunctionToMap {
  def main(args: Array[String]) = {
    val str = "gwdwhqkdhqjldj"
    countchar(str)
    print("completed")

  }

  def countchar(str: String): MutableList[String] = {
    val list = scala.collection.mutable.MutableList[String]()
    val lst = scala.collection.mutable.MutableList[String]()
    for (i <- str) {
      list += i.toString()
    }
    println(list)
    val s = list.map(x => (x, 1)).groupBy(_._1)
    val list1 = list.map(x => (x.split(" ")(0), 1)).toList
    val list2 = list1.groupBy(_._1).mapValues(x => x.map(_._2).reduce(_+_))
    val list3 = list2.map(x => mul(x))
    
    for (x <- list2.keys) {
      var s = ""
      s= list2(x).toString().+(x)
      lst += s
    }
    
    println(list3)
    list
  }
  
  var mul = (a : (String, Int)) => a._2.+(a._1)
 
}