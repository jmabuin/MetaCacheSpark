package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/21/17.
 */
public class SearchInHashMap implements FlatMapFunction<Iterator<HashMap<Integer, List<LocationBasic>>>, LocationBasic> {

	private int searchedValue = 0;

	public SearchInHashMap(int searchedValue) {
		this.searchedValue = searchedValue;
	}

	@Override
	public Iterator<LocationBasic> call(Iterator<HashMap<Integer, List<LocationBasic>>> myHashMap) {

		// Theoretically there is only one HashMap per partition
		while(myHashMap.hasNext()) {

			HashMap<Integer, List<LocationBasic>> currentHashMap = myHashMap.next();

			if (currentHashMap.containsKey(this.searchedValue)) {

				return currentHashMap.get(this.searchedValue).iterator();
			}
			else {
				return new ArrayList<LocationBasic>().iterator();
			}

		}

		return new ArrayList<LocationBasic>().iterator();

	}

}
