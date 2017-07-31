package com.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Util {
  
  def TrimAllColumns (df : DataFrame) : DataFrame = {
    
    var new_df = df
    
   val cols = df.columns.toList
   
   cols.foreach( a =>
       
       new_df = new_df.withColumn(a, col(a))
       
   )
   
   new_df
    
  }
  
  def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
    /*val columns = a.dtypes.toSet.intersect(b.dtypes.toSet).map{case (c, _) => col(c)}.toSeq
    a.select(columns: _*).unionAll(b.select(columns: _*))*/
    val x = a.columns.toSet.toArray.map{ case (key) => key.toLowerCase }
    val y = b.columns.toSet.toArray.map{ case (key) => key.toLowerCase }
    val columns = x.intersect(y).map(col).toSeq
    a.select(columns: _*).unionAll(b.select(columns: _*))
  } 
  
       def unionofSequence(dataFrames: Seq[DataFrame]): DataFrame = {
      var Arr1 = Array[String]()
      var Arr2 = Array[String]()
     // var columns = Seq[Column]()
      var columns_array = Array[String]()
      var a = 1
      for ( i <-  dataFrames){
        if( a == 1){
           Arr1 = i.columns.toSet.toArray.map{ case (key) => key.toLowerCase }
            a = a+1
                   }
        else {
         Arr2 = i.columns.toSet.toArray.map{ case (key) => key.toLowerCase }
         columns_array = Arr1.intersect(Arr2)  
         Arr1 = columns_array
        }
      }
       val columns= columns_array.map(col).toSeq
     dataFrames.reduceLeft(_.select(columns:_*) unionAll _.select(columns:_*) )
  } 
}