package com.scripts

import scala.util.control.Breaks;
import com.google.gson.annotations.Until

object countOfDigit {
  def main(args: Array[String]) = {
  //val a = Array(5,3,9,2,18,14,10)
  
  // Array(3,5,9,2,18,14,10)
  
  //sorting(a)
var inc = (x:Int) => x+1

println(mul(inc,8))

  }
 
/*def sorting( Arr : Array[Int]) = {
    var temp : Int = 1

for(i <- 0 to Arr.length-1)
{
for( j <- i+1 to Arr.length-1 ){
if(Arr(i)>Arr(j)){
temp = Arr(i);
Arr(i)=Arr(j);
Arr(j)=temp;
      }
     }
    } */
  
 def mul(f:Int => Int, a:Int) :Int = {
   a * (f(a)+ a)
 }
 def sum(f: Int => Int, a: Int, b: Int): Int = 
  if (a > b) 0
  else f(a) + sum(f, a + 1, b) 
  
  def id(x: Int): Int = x
}
