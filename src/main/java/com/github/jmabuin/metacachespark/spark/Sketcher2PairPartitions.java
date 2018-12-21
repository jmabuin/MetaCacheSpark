/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.*;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by chema on 1/16/17.
 */
//public class Sketcher2Pair implements FlatMapFunction<Iterator<Sequence>,HashMap<Integer, ArrayList<LocationBasic>>> {
public class Sketcher2PairPartitions implements FlatMapFunction<Iterator<Sequence>, HashMultiMapNative> {

	private static final Log LOG = LogFactory.getLog(Sketcher2PairPartitions.class);

	private TreeMap<String, Integer> sequencesIndexes;
	private int firstEmptyPosition;


	public Sketcher2PairPartitions(TreeMap<String, Integer> sequencesIndexes) {

		this.sequencesIndexes = sequencesIndexes;
		//this.firstEmptyPosition = 0;
		//this.lookup = new HashMap<Integer, Integer>();
	}

	@Override
	public Iterator<HashMultiMapNative> call(Iterator<Sequence> inputSequences){


		ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();
		HashMultiMapNative map = new HashMultiMapNative();

		int currentStart;
		int currentEnd;

		//String currentWindow;
		int numWindows;
		int current_sketch_size;

		ArrayList<Tuple2<Integer, LocationBasic>> returnedValues_local = new ArrayList<Tuple2<Integer, LocationBasic>>();

		while(inputSequences.hasNext()) {
			Sequence inputSequence = inputSequences.next();


			currentStart = 0;
			currentEnd = MCSConfiguration.windowSize;

			//String currentWindow = "";
			//StringBuffer currentWindow = new StringBuffer();
			numWindows = 0;

			//LOG.warn("Processing sequence: " + inputSequence.getHeader());
			// We iterate over windows (with overlap)

			while (currentStart < (inputSequence.getData().length() - MCSConfiguration.kmerSize)) {
				//Sketch resultSketch = new Sketch();

				if (currentEnd > inputSequence.getData().length()) {
					currentEnd = inputSequence.getData().length();
				}

				current_sketch_size = MCSConfiguration.sketchSize;

				if ((currentEnd - currentStart) >= MCSConfiguration.kmerSize) {

					if (currentEnd - currentStart < MCSConfiguration.kmerSize * 2){
						current_sketch_size = currentEnd - currentStart - MCSConfiguration.kmerSize + 1;
					}

					// We compute the k-mers. In C
					int sketchValues[] = HashFunctions.window2sketch32(inputSequence.getData().substring(currentStart, currentEnd)
							, current_sketch_size, MCSConfiguration.kmerSize);

					if (sketchValues != null) {
						//LOG.warn("[JMAbuin] CurrentWindow sketch size: " + sketchValues.length);

						for (int newValue : sketchValues) {

							//LOG.warn("Calculated value: " + newValue);
							returnedValues_local.add(new Tuple2<Integer, LocationBasic>(newValue,
									new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows)));


						}
					}


				}
				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}


			for(Tuple2<Integer, LocationBasic> current_loc: returnedValues_local) {

				map.add(current_loc._1(), current_loc._2().getTargetId(), current_loc._2().getWindowId());

			}

			returnedValues_local.clear();


		}

		int total_deleted = map.post_process(false, false);

		LOG.warn("Number of deleted features: " + total_deleted);

		returnedValues.add(map);


		return returnedValues.iterator();
	}


	public void insert(ArrayList<Tuple2<Integer, List<LocationBasic>>> returnedValues, int key, int id, int window) {

		int currentMapSize = returnedValues.size();
		int pos = Math.abs(key) % currentMapSize; // H2 function

		if(returnedValues.get(pos) == null) { // Empty position. Add current key and value
			List<LocationBasic> newList = new ArrayList<LocationBasic>();
			newList.add(new LocationBasic(id, window));

			returnedValues.set(pos, new Tuple2<Integer, List<LocationBasic>>(key, newList));
			//LOG.warn("Found empty at " + this.firstEmptyPosition + " with pos "+pos);

			if(this.firstEmptyPosition == pos) {
				this.firstEmptyPosition++;
			}


			//while((this.firstEmptyPosition < returnedValues.size()) && (returnedValues.get(this.firstEmptyPosition) != null)) {
			//	this.firstEmptyPosition++;
			//}

		}
		else if(returnedValues.get(pos)._1() == key) { // Busy position with same key. Append value if possible ( vals < 256)
			if(returnedValues.get(pos)._2().size() < 256) {
				returnedValues.get(pos)._2().add(new LocationBasic(id, window));
			}

		}
		else { // Our position is occupied with another key. Insert into first empty position

			int previousSize = returnedValues.size();

			if(this.firstEmptyPosition >= previousSize) { // Reached end, increase size * 1.5


				int newSize = (int)(previousSize * 1.5);

				//LOG.warn("Increasing from " +previousSize+" to " + newSize +"and empty is "+this.firstEmptyPosition);

				//returnedValues.ensureCapacity(newSize);

				for(int j = previousSize; j <= newSize; j++) {
					//returnedValues.add(new Tuple2<Integer, List<LocationBasic>>(-1, null));
					returnedValues.add(null);
				}


			}

			List<LocationBasic> newList = new ArrayList<LocationBasic>();
			newList.add(new LocationBasic(id, window));

			returnedValues.set(this.firstEmptyPosition, new Tuple2<Integer, List<LocationBasic>>(key, newList));
			//this.firstEmptyPosition++;

			while((this.firstEmptyPosition < returnedValues.size()) && (returnedValues.get(this.firstEmptyPosition) != null)) {
				this.firstEmptyPosition++;
			}

/*
			int i;

			for(i = pos; i< returnedValues.size(); i++) {
				if(returnedValues.get(i) == null) { // Found new empty bucket. Insert there
					List<LocationBasic> newList = new ArrayList<LocationBasic>();
					newList.add(new LocationBasic(id, window));

					returnedValues.set(i, new Tuple2<Integer, List<LocationBasic>>(key, newList));

					break;
				}
				//else if (returnedValues.get(i)._1() == key) { // Maybe we find our key in another position from  a previous insert
				//	if(returnedValues.get(pos)._2().size() < 256) {
				//		returnedValues.get(pos)._2().add(new LocationBasic(id, window));
				//	}
				//	break;
				//}
			}

			if(i == returnedValues.size()) { // Reached end of ArrayList. Increase its size and insert element in first empty position

				int previousSize = returnedValues.size();
				int newSize = (int)(previousSize * 1.5);

				//returnedValues.ensureCapacity(newSize);

				for(int j = previousSize; j < newSize; j++) {
					//returnedValues.add(new Tuple2<Integer, List<LocationBasic>>(-1, null));
					returnedValues.add(null);
				}

				List<LocationBasic> newList = new ArrayList<LocationBasic>();
				newList.add(new LocationBasic(id, window));

				returnedValues.set(previousSize, new Tuple2<Integer, List<LocationBasic>>(key, newList));


			}
*/
		}

	}



}
