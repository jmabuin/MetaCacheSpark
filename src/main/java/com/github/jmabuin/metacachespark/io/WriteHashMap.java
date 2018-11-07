package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.util.*;

/**
 * Created by chema on 3/22/17.
 */
public class WriteHashMap implements Function2<Integer, Iterator<HashMap<Integer, List<LocationBasic>>>, Iterator<String>> {

	private static final Log LOG = LogFactory.getLog(WriteHashMap.class);

	private String path;

	public WriteHashMap(String path) {
		this.path = path;
	}

	@Override
	public Iterator<String> call(Integer partitionId, Iterator<HashMap<Integer, List<LocationBasic>>> values) {


		String filename = this.path+"/part-"+partitionId;
		String local_filename = "part-"+partitionId;
		ArrayList<String> returnValues = new ArrayList<String>();

		try {


			FileOutputStream fileOut = new FileOutputStream(local_filename);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			while(values.hasNext()) {
				HashMap<Integer, List<LocationBasic>> currentMap = values.next();
				out.writeObject(currentMap);

			}

			out.close();
			fileOut.close();

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			fs.copyFromLocalFile(new Path(local_filename), new Path(filename));

            returnValues.add(filename);

/*
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			returnValues.add(filename);

			FSDataOutputStream outputStream = fs.create(new Path(filename));

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

			// Write data
			StringBuffer currentLine = new StringBuffer();

			while(values.hasNext()) {
				HashMap<Integer, List<LocationBasic>> currentMap = values.next();

				// Theoretically there is only one HashMap per partition
				bw.write(String.valueOf(currentMap.size()));
				bw.newLine();

				for(Map.Entry<Integer, List<LocationBasic>> currentEntry: currentMap.entrySet()) {

					currentLine.append(currentEntry.getKey());
					//currentLine.append(":");
					currentLine.append(",");

					for(LocationBasic currentLocation: currentEntry.getValue()) {
						currentLine.append(currentLocation.getTargetId());
						currentLine.append(",");
						currentLine.append(currentLocation.getWindowId());
						//currentLine.append(";");
						currentLine.append(",");
					}


					bw.write(currentLine.toString());
					bw.newLine();

					currentLine.delete(0, currentLine.toString().length());

				}

			}


			bw.close();
			outputStream.close();
*/
			return returnValues.iterator();

		}
		catch (IOException e) {
			LOG.error("Could not write file "+ filename+ " because of IO error in writeSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}
		catch (Exception e) {
			LOG.error("Could not write file "+ filename+ " because of IO error in writeSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}

		return returnValues.iterator();

	}

}
