package com.kafka

import java.io.Writer
import java.util.Arrays
import java.util.UUID
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

;
case class convert(words: String)

object sparkstreaming {
     val sc = SparkSession.builder
    .master("local")
    .appName("kmeans")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/sumeet.agrawal/workspace/Sumeet_Spark")
    .getOrCreate()
    val ssc = new StreamingContext(sc.sparkContext, Seconds(10))
  def main(args:Array[String]) = {

    val topic = "carriers"
    val path = "hdfs://localhost:9000//user/sumeet/data/raw/"
    
   processEngine()
import sc.sqlContext.  implicits._

def createKafkaStream(ssc: StreamingContext, kafkaTopics: String, brokers: String): InputDStream[ConsumerRecord[String, String]] = {
  val topicsSet = kafkaTopics.split(",").toSet
    val props = Map[String, String](
        "bootstrap.servers" -> "localhost:9092",
        "metadata.broker.list" -> "localhost:9092",
        "serializer.class" -> "kafka.serializer.StringEncoder",
        "group.id" -> s"${UUID.randomUUID().toString}",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
 val props1 = new java.util.Properties()
  props foreach { case (key,value) => props1.put(key, value)}
  
   val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topic),props1.asInstanceOf[java.util.Map[String, Object]] ) )
          
   kafkaStream
}

def processEngine(): StreamingContext = {
    val ssc = new StreamingContext   (sc.sparkContext, Seconds(1))

    val topicStream = createKafkaStream(ssc, topic, "localhost:9092")
    topicStream.map(record=>(record.value().toString)).print
     val wordsDataFrame = topicStream.map(record=>(record.value().toString))  //.toDF("word")
   import sc.sqlContext.implicits._
    wordsDataFrame.foreachRDD { rdd =>
                      val message: RDD[String] = rdd.map { y => y }
                      val sqlContext =
                       
                        SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
                      import sqlContext.implicits._
                      val df :DataFrame = message.toDF("Column")
                      df.show()
                      if(df.count() > 0){
                      df.coalesce(1).write.format("com.databricks.spark.csv").mode("append").save(path+topic) }
                    }
    ssc
}

StreamingContext.getActive.foreach {
    _.stop(stopSparkContext = false)
}

val ssc1 = StreamingContext.getActiveOrCreate(processEngine)
ssc1.start()
ssc1.awaitTermination()

  }
}

object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}