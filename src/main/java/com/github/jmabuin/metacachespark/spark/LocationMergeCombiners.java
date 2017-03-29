package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by chema on 3/7/17.
 */
public class LocationMergeCombiners implements Function2<List<LocationBasic>, List<LocationBasic>, List<LocationBasic>> {

	@Override
	public List<LocationBasic> call(List<LocationBasic> v1, List<LocationBasic> v2){

		if((v1.size() + v2.size()) <= 256) {
			v1.addAll(v2);
		}
		else if (v1.size() <=256) {

			int i = 0;

			while((i < v2.size()) &&  (v1.size() <= 256)) {
				v1.add(v2.get(i));
			}

		}


		return v1;
	}

}
