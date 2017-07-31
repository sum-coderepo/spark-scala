package com.scripts

import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

 object class_validation {
     val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .enableHiveSupport()
    .getOrCreate()
 def main(args: Array[String]) = {
// this simply prints the number 10
//println("sum ints 1 to 4 = " + sumInts(1,4))
       
  var input = sc.sqlContext.createDataFrame(Seq(
    (3, 23, 40000),
    (3, 24, 30000),
    (1, 21, 10000),
    (1, 20, 3000),
    (2, 27, 5500),
    (2, 30, 90000),
    (2, 25, 123442),
    (3, 22, 30393),
    (3, 18, 3217834)  
     )).toDF("id", "AGE", "sal")
     
     input.show()
     input.sort(asc("id"), desc("AGE")).show()
}
def sum(f: Int => Int, a: Int, b: Int): Int = if (a > b) 0 else f(a) + sum(f, a + 1, b)

// these three functions use the sum() function
def sumInts(a: Int, b: Int): Int = sum(id, a, b)
def sumSquares(a: Int, b: Int): Int = sum(square, a, b)
def sumPowersOfTwo(a: Int, b: Int): Int = sum(powerOfTwo, a, b)

// three functions that are passed into the sum() function
def id(x: Int): Int = x
def square(x: Int): Int = x * x
def powerOfTwo(x: Int): Int = if (x == 0) 1 else 2 * powerOfTwo(x - 1)
}