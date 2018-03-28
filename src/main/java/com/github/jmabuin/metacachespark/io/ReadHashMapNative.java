package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by chema on 3/28/17.
 */
public class ReadHashMapNative implements Function2<Integer, Iterator<String>, Iterator<HashMultiMapNative>> {

	private static final Log LOG = LogFactory.getLog(ReadHashMapNative.class);

	private String path;



	@Override
	public Iterator<HashMultiMapNative> call(Integer partitionId, Iterator<String> values) {
		String filename;
		String onlyFileName;
		ArrayList<HashMultiMapNative> returnValues = new ArrayList<HashMultiMapNative>();

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			while (values.hasNext()) {


				filename = values.next();
				String parts[] = filename.split("/");
				onlyFileName = parts[parts.length-1];

				LOG.warn("Processing file: " + filename+" <> "+onlyFileName);

				HashMultiMapNative newMap = new HashMultiMapNative();

				fs.copyToLocalFile(new Path(filename), new Path(onlyFileName));

				newMap.read(onlyFileName);

				returnValues.add(newMap);

				File file = new File(onlyFileName);

				file.delete();
			}

			return returnValues.iterator();
		}
		catch (IOException e) {
			LOG.error("Could not read file because of IO error in writeSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}
		catch (Exception e) {
			LOG.error("Could not read file because of IO error in writeSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}

		return returnValues.iterator();

	}

}
