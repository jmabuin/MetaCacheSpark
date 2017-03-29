package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chema on 3/7/17.
 */

public class LocationCombiner implements Function<LocationBasic, List<LocationBasic>> {

	@Override
	public List<LocationBasic> call(LocationBasic currentLocation) {

		List<LocationBasic> newList = new ArrayList<LocationBasic>();

		newList.add(currentLocation);

		return newList;

	}

}