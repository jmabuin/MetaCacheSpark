package com.github.jmabuin.metacachespark.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/28/17.
 */
public class MyWholeTextFiles implements PairFlatMapFunction<Iterator<String>, String, String> {


	private static final Log LOG = LogFactory.getLog(MyWholeTextFiles.class);

	@Override
	public Iterable<Tuple2<String, String>> call(Iterator<String> fileNames) {

		List<Tuple2<String, String>> returnValues = new ArrayList<Tuple2<String, String>>();

		StringBuilder content = new StringBuilder();

		try {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			while(fileNames.hasNext()) {



				content.delete(0, content.toString().length());

				String fileName = fileNames.next();



				if(!fileName.contains("assembly_summary")) {

					String key = fileName;
					FSDataInputStream inputStream = fs.open(new Path(fileName));

					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

					String currentLine;

					while ((currentLine = br.readLine()) != null) {
						content.append(currentLine+"\n");
					}

					br.close();
					inputStream.close();

					//LOG.warn("Reading file: "+fileName+" - " + content.length());

					returnValues.add(new Tuple2<String, String>(key, content.toString()));
				}



			}


			return returnValues;
		}
		catch(IOException e) {
			LOG.error("Could not acces to HDFS");
			System.exit(-1);
		}


		return returnValues;

	}

}
