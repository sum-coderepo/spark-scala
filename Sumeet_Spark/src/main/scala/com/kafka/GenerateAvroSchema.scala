package com.kafka

import java.io.File;
import java.io.IOException;
 
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.avro.mapred.FsInput
import org.apache.avro.file.FileReader
import org.apache.avro.Schema

object GenerateAvroSchema {
  def main(args : Array[String]) = {

    val reader: GenericDatumReader[Any] = new GenericDatumReader[Any]()
    
    // final Path path = new Path(args[0]);
		// final FsInput input = new FsInput(path, new Configuration());
    val input: File = new File(
      "C:\\Users\\sumeet.agrawal\\workspace\\test.avro\\test.avro")
    val fileReader: FileReader[Any] = DataFileReader.openReader(input, reader)
    try {
      val schema1: Schema = fileReader.getSchema
      println(schema1)
    } finally fileReader.close()
    
    
  }
  }