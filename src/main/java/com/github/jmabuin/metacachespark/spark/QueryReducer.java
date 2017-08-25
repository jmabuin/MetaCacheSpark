package com.github.jmabuin.metacachespark.spark;

import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducer implements Function2<List<int[]>, List<int[]>, List<int[]>> {

	@Override
	public List<int[]>call(List<int[]> v1, List<int[]> v2) {

		if(v1.size() + v2.size() < 256) {
			v1.addAll(v2);
		}
		else {
			if (v1.size() <= 256) {

				int currentSize;
				int i;
				for(currentSize = v1.size(), i = 0; currentSize < 256 && i< v2.size(); currentSize++, i++) {
					v1.add(v2.get(i));
				}
			}

		}
		return v1;
	}

}
