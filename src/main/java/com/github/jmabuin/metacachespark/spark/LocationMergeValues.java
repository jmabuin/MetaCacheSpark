package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chema on 3/7/17.
 */
/*public class LocationMergeValues implements Function2<List<LocationBasic>, LocationBasic, List<LocationBasic>> {

	@Override
	public List<LocationBasic> call(List<LocationBasic> list, LocationBasic value) {

		list.add(value);

		return list;
	}

}
*/

public class LocationMergeValues implements Function2<List<LocationBasic>, LocationBasic, List<LocationBasic>> {

	@Override
	public List<LocationBasic> call(List<LocationBasic> list, LocationBasic value) {

		if(list.size() <= 256) {
			list.add(value);
		}


		return list;
	}

}