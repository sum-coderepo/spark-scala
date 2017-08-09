name := "spark-scala" 

crossPaths := false 

name := "Sumeet_Spark"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

organization := "com.janssen"
 
 
libraryDependencies ++= {
     Seq(
	 "com.github.jcustenborder.kafka.connect" % "connect-utils" % "0.2.81",
	 "junit" % "junit" % "3.8.1" % "test",
	 "com.databricks" % "spark-xml_2.11" % "0.4.1",
	 "org.scala-lang" % "scala-library" % "2.11.8",
	 "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided",
	 "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided",
	 "org.apache.spark" % "spark-hive_2.11" % "2.1.0",
	 "com.databricks" % "spark-csv_2.11" % "1.5.0",
	 "com.google.guava" % "guava" % "18.0",
	 "com.databricks" % "spark-avro_2.11" % "3.1.0",
	 "org.apache.spark" % "spark-mllib_2.11" % "2.0.2",
	 "com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.5.0",
	 "org.scalatest" % "scalatest_2.11" % "3.0.1",
	 "org.apache.hadoop" % "hadoop-common" % "2.7.1",
	 "com.twitter" % "algebird-core_2.11" % "0.12.4",
	 "com.typesafe.akka" % "akka-actor_2.11" % "2.4.16",
	 "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.6.4",
	 "com.databricks" % "spark-redshift_2.11" % "2.0.1",
	 "com.amazonaws" % "aws-java-sdk" % "1.11.68",
	 "org.apache.hadoop" % "hadoop-aws" % "2.7.1",
	 "com.fasterxml.jackson.core" % "jackson-annotations" % "2.8.5",
	 "net.java.dev.jets3t" % "jets3t" % "0.9.4",
	 "org.apache.spark" % "spark-catalyst_2.11" % "2.1.1" % "provided",
	 "com.amazonaws" % "aws-encryption-sdk-java" % "0.0.1",
	 "com.amazonaws" % "aws-java-sdk" % "1.2.1",
	 "com.amazonaws" % "aws-java-sdk-kms" % "1.11.158",
	 "net.sourceforge.argparse4j" % "argparse4j" % "0.7.0" % "provided",
	 "com.opencsv" % "opencsv" % "3.8",
	 "com.fasterxml.jackson.core" % "jackson-core" % "2.5.0",
	 "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.0",
	 "com.fasterxml.jackson.core" % "jackson-annotations" % "2.5.0",
//	 "org.reflections" % "reflections-maven" % "0.9.9-RC2",
	 "org.apache.kafka" % "connect-api" % "0.9.0.0",
	 "org.apache.avro" % "avro-tools" % "1.7.7",
	 "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0"
//	 "org.jfrog.jade.plugins.common" % "jade-plugin-common" % "1.3.8"
    ) 
 }
 
parallelExecution in test := false 

//resolvers ++= Seq("snapshots"     at "http://oss.sonatype.org/content/repositories/snapshots",
//                "releases"        at "http://oss.sonatype.org/content/repositories/releases"
//                )
 
scalacOptions ++= Seq("-unchecked", "-deprecation")
