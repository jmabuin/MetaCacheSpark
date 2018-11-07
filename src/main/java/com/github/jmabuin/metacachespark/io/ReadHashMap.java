package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/28/17.
 */
public class ReadHashMap implements Function2<Integer, Iterator<String>, Iterator<HashMap<Integer, List<LocationBasic>>>> {

	private static final Log LOG = LogFactory.getLog(ReadHashMapNative.class);

	private String path;



	@Override
	public Iterator<HashMap<Integer, List<LocationBasic>>> call(Integer partitionId, Iterator<String> values) {

		String filename;
		String onlyFileName;
		ArrayList<HashMap<Integer, List<LocationBasic>>> returnValues = new ArrayList<HashMap<Integer, List<LocationBasic>>>();

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			while (values.hasNext()) {


				filename = values.next();
				String parts[] = filename.split("/");
				onlyFileName = parts[parts.length-1];

				LOG.info("Processing file to insert into Java hashmap: " + filename+" <> "+onlyFileName);

                HashMap<Integer, List<LocationBasic>> newMap = null;//new HashMap<Integer, List<LocationBasic>>();

				fs.copyToLocalFile(new Path(filename), new Path(onlyFileName));

                FileInputStream fileIn = new FileInputStream(onlyFileName);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                newMap = (HashMap<Integer, List<LocationBasic>>) in.readObject();
                in.close();
                fileIn.close();


				//newMap.read(onlyFileName);

				returnValues.add(newMap);

				File file = new File(onlyFileName);

				file.delete();
			}

			return returnValues.iterator();
		}
		catch(ClassCastException e) {
            LOG.error("Could not read file because of Cast error in writeSid2gid.");
            e.printStackTrace();
        }

		catch (IOException e) {
			LOG.error("Could not read file because of IO error in writeSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}
		catch (Exception e) {
			LOG.error("Could not read file because of General error in writeSid2gid.");
			e.printStackTrace();
			//System.exit(1);
		}

		return returnValues.iterator();

	}

}
