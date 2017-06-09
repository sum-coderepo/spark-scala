package com.scripts

import java.io.File
import org.apache.spark.sql.SparkSession

object class_validation1 {
     val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .enableHiveSupport()
    .getOrCreate()
 def main(args: Array[String]) = {
// this simply prints the number 10
println("sum ints 1 to 4 = " + sumInts(1,4))
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