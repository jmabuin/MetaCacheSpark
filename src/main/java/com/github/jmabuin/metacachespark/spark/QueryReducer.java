package com.github.jmabuin.metacachespark.spark;

import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducer implements Function2<List<int[]>, List<int[]>, List<int[]>> {

	@Override
	public List<int[]>call(List<int[]> v1, List<int[]> v2) {
		v1.addAll(v2);
		return v1;
	}

}
