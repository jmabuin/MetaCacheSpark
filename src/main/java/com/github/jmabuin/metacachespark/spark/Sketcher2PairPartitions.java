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
public class Sketcher2PairPartitions implements FlatMapFunction<Iterator<Sequence>, List<Location>> {

	private static final Log LOG = LogFactory.getLog(Sketcher2Pair.class);
	private short k_;
	private int sketchSize_;

	public int hash_(int x) {
		return HashFunctions.thomas_mueller_hash(x);
	}

	public byte max_kmer_size(byte bitsPerSymbol) {
		return (byte)((8 * 8) / bitsPerSymbol);
	}

	int max_sketch_size() {
		return Integer.MAX_VALUE;
	}

	private TreeMap<String, Integer> sequencesIndexes;
	private HashMap<Integer, Integer> lookup;
	private int firstEmptyPosition;


	public Sketcher2PairPartitions(TreeMap<String, Integer> sequencesIndexes) {

		this.sequencesIndexes = sequencesIndexes;
		//this.firstEmptyPosition = 0;
		//this.lookup = new HashMap<Integer, Integer>();
	}

	@Override
	public Iterator<List<Location>> call(Iterator<Sequence> inputSequences){

		ArrayList<List<Location>> returnedValues = new ArrayList<List<Location>>();

		long initTime = System.nanoTime();
		long endTime;

		//while(inputSequences.hasNext()){
		//for(Sequence inputSequence: inputSequences) {

		//Sequence inputSequence = inputSequences.next();

		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		String currentWindow = "";
		int numWindows = 0;
		int current_sketch_size = MCSConfiguration.sketchSize;

		while(inputSequences.hasNext()) {
			Sequence inputSequence = inputSequences.next();
			ArrayList<Location> seq_returnedValues = new ArrayList<Location>();

			currentStart = 0;
			currentEnd = MCSConfiguration.windowSize;

			currentWindow = "";
			numWindows = 0;

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


					//LOG.warn("[JMAbuin] Current window is: " + currentWindow);
					// We compute the k-mers. In C
					int[] sketchValues = HashFunctions.window2sketch32(inputSequence.getData().substring(currentStart, currentEnd)
							, current_sketch_size, MCSConfiguration.kmerSize);

					if (sketchValues != null) {
						//LOG.warn("[JMAbuin] CurrentWindow sketch size: " + sketchValues.length);

						for (int newValue : sketchValues) {

							//LOG.warn("Calculated value: " + newValue);
							seq_returnedValues.add(
									new Location(newValue, this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows));


						}
					}


				}
				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}

			returnedValues.add(seq_returnedValues);
			//LOG.warn("[JMAbuin] Total windows: "+numWindows);

		}

		endTime = System.nanoTime();
		LOG.warn("Time for this partition is: " + ((endTime - initTime)/1e9));


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
