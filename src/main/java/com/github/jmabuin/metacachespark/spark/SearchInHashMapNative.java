package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/28/17.
 */
public class SearchInHashMapNative implements FlatMapFunction<Iterator<HashMultiMapNative>, LocationBasic> {

	private int searchedValue = 0;

	public SearchInHashMapNative(int searchedValue) {
		this.searchedValue = searchedValue;
	}

	@Override
	public Iterable<LocationBasic> call(Iterator<HashMultiMapNative> myHashMaps) {

		List<LocationBasic> returnValues = new ArrayList<LocationBasic>();

		// Theoretically there is only one HashMap per partition
		while(myHashMaps.hasNext()) {

			HashMultiMapNative currentHashMap = myHashMaps.next();

			int[] values = currentHashMap.get(this.searchedValue);

			if(values != null) {

				for(int i = 0; i< values.length; i+=2) {

					returnValues.add(new LocationBasic(values[i], values[i+1]));
				}

			}

		}

		return returnValues;

	}

}