package com.github.metacachespark;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by chema on 1/31/17.
 */
public class Sketch2Features implements FlatMapFunction<Iterator<Sketch>, Feature> {

	@Override
	public Iterable<Feature> call(Iterator<Sketch> sketchIterator) throws Exception {

		ArrayList<Feature> returnedValues = new ArrayList<Feature>();

		while(sketchIterator.hasNext()) {
			Sketch currentSketch = sketchIterator.next();

			for(Feature currentFeature: currentSketch.getFeatures()) {
				returnedValues.add(currentFeature);
			}


		}


		return returnedValues;
	}

}
