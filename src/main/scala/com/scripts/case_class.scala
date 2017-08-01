package com.scripts

import scala.util.{Success, Failure, Try}

/**
 * Created by ganesh on 30/1/17.
 */

case class Planet(name: String, satellites: List[String], mass: Float)
  
object UsingOptions {
  def convertToInt(str:String) : Option[Int] = {
    val integerValue = Try(str.toInt)
    integerValue match {
      case Failure(exception) => {
        println(s"Cannot convert $str to integer returnin g default value 0")
        None
      }
      case Success(number) => {
        println(s"successfully converted string $str to integer")
        Some(number)
      }
    }
  }



  def main(args: Array[String]) {
    val stringValue = "10"
    convertToInt(stringValue).getOrElse(0)

    val stringInvalidValue = "hello"
    convertToInt(stringInvalidValue).getOrElse(0)
    
    val a = testing_case_class("HI",List("Hello","How","Are","Yu"),7.8.floatValue())
    
    val b = println(a.mass)
    val c = println(a.name)
    val d = println(a.satellites)
    
    println(newfunc(a))
  }
  
  
  def testing_case_class(nm : String,st :List[String], ms : Float) : Planet = {
    new Planet(nm,st,ms)
  }
  
  def newfunc( test : Planet) : String = {
    
    val str = test.name
    str
    
  }
  
}