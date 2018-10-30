package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by chema on 4/4/17.
 */
public class Pair2HashMapNative implements Function2<Integer, Iterator<Tuple2<Integer, LocationBasic>>, Iterator<HashMultiMapNative>> {

	private static final Log LOG = LogFactory.getLog(Pair2HashMapNative.class);

	@Override
	public Iterator<HashMultiMapNative> call(Integer partitionId, Iterator<Tuple2<Integer, LocationBasic>> tuple2Iterator) throws Exception {

		LOG.warn("Starting to process partition: "+partitionId);

		int initialSize = 5;

		ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();

		HashMultiMapNative map = new HashMultiMapNative();

		while(tuple2Iterator.hasNext()) {
			Tuple2<Integer, LocationBasic> currentItem = tuple2Iterator.next();

			map.add(currentItem._1(), currentItem._2().getTargetId(), currentItem._2().getWindowId());

		}

		returnedValues.add(map);

		return returnedValues.iterator();
	}
}
