package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/23/17.
 */
public class ReadHashMapPartitions implements FlatMapFunction<Iterator<String>, HashMap<Integer, List<LocationBasic>>> {

	private static final Log LOG = LogFactory.getLog(ReadHashMapPartitions.class);

	@Override
	public Iterable<HashMap<Integer, List<LocationBasic>>> call(Iterator<String> fileNames) {

		List<HashMap<Integer, List<LocationBasic>>> returnValue = new ArrayList<HashMap<Integer, List<LocationBasic>>>();


		while(fileNames.hasNext()){

			String fileName = fileNames.next();

			HashMap<Integer, List<LocationBasic>> currentHashMap = new HashMap<Integer, List<LocationBasic>>();

			try {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream inputStream = fs.open(new Path(fileName));

				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

				// read data
				long numberItems = Long.parseLong(br.readLine());

				String currentLine;
				int lineNum = 0;

				while ((currentLine = br.readLine()) != null) {

					String parts[] = currentLine.split(",");

					int key = Integer.parseInt(parts[0]);

					List<LocationBasic> myLocations = new ArrayList<LocationBasic>();

					for (int i = 1; i < parts.length; i += 2) {
						myLocations.add(new LocationBasic(Integer.parseInt(parts[i]), Integer.parseInt(parts[i+1])));
					}

					currentHashMap.put(key, myLocations);

				}


				br.close();
				inputStream.close();

				returnValue.add(currentHashMap);
			} catch (IOException e) {
				LOG.error("Could not write file " + fileName + " because of IO error in readSid2gid.");
				e.printStackTrace();
				//System.exit(1);
			} catch (Exception e) {
				LOG.error("Could not write file " + fileName + " because of IO error in readSid2gid.");
				e.printStackTrace();
				//System.exit(1);
			}

		}

		return returnValue;

	}

}
