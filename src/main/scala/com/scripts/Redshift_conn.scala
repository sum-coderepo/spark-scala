package com.scripts
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Redshift_conn {
    def main(args: Array[String]): Unit = {

      val awsAccessKeyId = "AKIAI4B3AO7WOPB7TWBQ"
      val awsSecretAccessKey = "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"
      val redshiftDBName = "dev"
      val redshiftUserId = "cesmartsrcadm"
      val redshiftPassword = "DLZaEY83J4QN"
      val redshifturl = "redshift-d-cesmart.cxpyts0ysmpv.us-east-1.redshift.amazonaws.com:5439"
      val jdbcURL = s"""jdbc:redshift://$redshifturl/$redshiftDBName?user=$redshiftUserId&password=$redshiftPassword"""
      val bucketName = "cesmartsourcing" //args(6)
      val tempS3Dir = "s3n://" + bucketName + "/tmp/"

      val sc = new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local"))

      // Configure SparkContext to communicate with AWS
    //  val tempS3Dir = "s3n://redshift-spark/temp/"
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

      // Create the SQL Context
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      val eventsDF = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcURL)
        .option("tempdir", tempS3Dir)
        .option("dbtable", "t_ddd_dollar")
        .load()
      eventsDF.show()
    }
}