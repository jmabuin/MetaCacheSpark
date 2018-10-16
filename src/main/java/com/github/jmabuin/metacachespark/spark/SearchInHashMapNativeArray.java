package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
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
public class SearchInHashMapNativeArray implements FlatMapFunction<Iterator<HashMultiMapNative>, LocationBasic> {

	private int searchedValues[];

	public SearchInHashMapNativeArray(int[] searchedValues) {
		this.searchedValues = searchedValues;
	}

	@Override
	public Iterator<LocationBasic> call(Iterator<HashMultiMapNative> myHashMaps) {

		List<LocationBasic> returnValues = new ArrayList<LocationBasic>();

		// Theoretically there is only one HashMap per partition
		//while(myHashMaps.hasNext()) {

			HashMultiMapNative currentHashMap = myHashMaps.next();


			for(int newKey: searchedValues) {
				int[] values = currentHashMap.get(newKey);

				if(values != null) {

					for(int i = 0; i< values.length; i+=2) {

						//returnValues.add(new Location(newKey, values[i], values[i+1]));
						returnValues.add(new LocationBasic(values[i], values[i+1]));
					}

				}
			}



		//}

		return returnValues.iterator();

	}

}