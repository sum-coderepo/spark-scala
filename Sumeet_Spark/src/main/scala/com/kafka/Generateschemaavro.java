package com.kafka;

import org.apache.avro.file.FileReader; 
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema; 
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class Generateschemaavro {

	 public static void main(String[] args) throws IOException {

		 
		 final GenericDatumReader<Object> reader = new GenericDatumReader<Object>() ;
		// final Path path = new Path(args[0]);
		// final FsInput input = new FsInput(path, new Configuration());
		// FileReader<Object> fileReader = DataFileReader.openReader(input, reader); 
		 
		 File input = new File("C:\\Users\\sumeet.agrawal\\workspace\\test.avro\\test.avro");
		 FileReader<Object> fileReader = DataFileReader.openReader(input, reader); 

		 try{
			 final Schema schema1 = fileReader.getSchema();
			 System.out.println(schema1);
		 }
		finally {
			   fileReader.close();
		  } 

	 }
}
