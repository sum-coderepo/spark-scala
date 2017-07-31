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

object GenerateAvroSchema {
  def main(args : Array[String]) = {
     
    val reader = new GenericDatumReader [_ <: D ]
    val path = new Path(args(0)) 
    val input = new FsInput(path , new Configuration)
    val fileReader = DataFileReader.openReader(input, reader)

        try
        {
            final Schema schema = fileReader.getSchema(); 
  }
}