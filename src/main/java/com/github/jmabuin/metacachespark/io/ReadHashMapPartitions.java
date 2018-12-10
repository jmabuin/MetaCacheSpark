/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

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
	public Iterator<HashMap<Integer, List<LocationBasic>>> call(Iterator<String> fileNames) {

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

		return returnValue.iterator();

	}

}
