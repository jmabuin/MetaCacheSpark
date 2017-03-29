package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.util.*;

/**
 * Created by chema on 3/22/17.
 */
public class WriteHashMapNative implements Function2<Integer, Iterator<HashMultiMapNative>, Iterator<String>> {

	private static final Log LOG = LogFactory.getLog(WriteHashMapNative.class);

	private String path;

	public WriteHashMapNative(String path) {
		this.path = path;
	}

	@Override
	public Iterator<String> call(Integer partitionId, Iterator<HashMultiMapNative> values) {
		String filename = "part-" + partitionId;
		ArrayList<String> returnValues = new ArrayList<String>();

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			int i = 0;

			while (values.hasNext()) {
				HashMultiMapNative currentMap = values.next();

				currentMap.write(filename + "_" + i);

				fs.copyFromLocalFile(true, true, new Path(filename + "_" + i), new Path(this.path + "/"+filename + "_" + i));

				i++;
			}

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
