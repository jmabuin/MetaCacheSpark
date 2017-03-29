package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.options.CommonOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/22/17.
 */
/*public class ReadHashMap implements FlatMapFunction<Iterator<Tuple2<String, String>>, HashMap<Integer, List<LocationBasic>>> {

	@Override
	public Iterable<HashMap<Integer, List<LocationBasic>>> call(Iterator<Tuple2<String, String>> fileContent) {

		List<HashMap<Integer, List<LocationBasic>>> returnValue = new ArrayList<HashMap<Integer, List<LocationBasic>>>();


		while(fileContent.hasNext()) {
			Tuple2<String, String> currentFile = fileContent.next();

			HashMap<Integer, List<LocationBasic>> currentHashMap = new HashMap<Integer, List<LocationBasic>>();

			String items[] = currentFile._2().split("\n");

			int numLine = 0;
			int mapSize = 0;
			for(String currentLine: items) {

				if(numLine == 0) {
					mapSize = Integer.parseInt(currentLine);
				}
				else {

					String locationItems[] = currentLine.split(":");

					int key = Integer.parseInt(locationItems[0]);
					String locations[] = locationItems[1].split(";");

					List<LocationBasic> myLocations = new ArrayList<LocationBasic>();

					for(String currentLocation: locations) {

						String thisLocationItems[] = currentLocation.split(",");

						myLocations.add(new LocationBasic(Integer.parseInt(thisLocationItems[0]), Integer.parseInt(thisLocationItems[1])));

					}

					currentHashMap.put(key, myLocations);
				}
				numLine++;


			}

			returnValue.add(currentHashMap);

		}

		return returnValue;


	}

}
*/

public class ReadHashMap implements Function<String, HashMap<Integer, List<LocationBasic>>> {

	private static final Log LOG = LogFactory.getLog(ReadHashMap.class);

	@Override
	public HashMap<Integer, List<LocationBasic>> call(String fileName) {


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

			return currentHashMap;
		} catch (IOException e) {
			LOG.error("Could not write file " + fileName + " because of IO error in readSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		} catch (Exception e) {
			LOG.error("Could not write file " + fileName + " because of IO error in readSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}

		return currentHashMap;
	}

}