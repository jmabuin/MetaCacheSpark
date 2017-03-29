package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/16/17.
 */
public class Sketcher2HashMap implements Function2<Integer, Iterator<Sequence> ,Iterator<HashMap<Integer, List<LocationBasic>>>> {

	private static final Log LOG = LogFactory.getLog(Sketcher2HashMap.class);
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

	private HashMap<String, Integer> sequencesIndexes;

	public Sketcher2HashMap(HashMap<String, Integer> sequencesIndexes) {

		this.sequencesIndexes = sequencesIndexes;
	}

	@Override
	public Iterator<HashMap<Integer, List<LocationBasic>>> call(Integer partitionId, Iterator<Sequence> inputSequences){

		//public Iterable<Integer,Location> call(Sequence inputSequence) {

		LOG.warn("Starting to process partition: "+partitionId);

		int initialSize = 5;

		ArrayList<HashMap<Integer, List<LocationBasic>>> returnedValues = new ArrayList<HashMap<Integer, List<LocationBasic>>>();

		HashMap<Integer, List<LocationBasic>> map = new HashMap<Integer, List<LocationBasic>>();

		returnedValues.add(map);

		long initTime = System.nanoTime();
		long endTime;

		//while(inputSequences.hasNext()){
		//for(Sequence inputSequence: inputSequences) {

		//Sequence inputSequence = inputSequences.next();

		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		String currentWindow = "";
		int numWindows = 0;

		while(inputSequences.hasNext()) {
			Sequence inputSequence = inputSequences.next();

			currentStart = 0;
			currentEnd = MCSConfiguration.windowSize;

			currentWindow = "";
			numWindows = 0;

			// We iterate over windows (with overlap)
			//while (currentEnd <= inputSequence.getData().length()) {

			LOG.warn("[JMAbuin] Processing sequence: "+inputSequence.getHeader()+ " with length " +
					inputSequence.getData().length());
			LOG.warn("Consumed memory: "+ ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024.0/1024.0)+" MB");
			while (currentStart < (inputSequence.getData().length() - MCSConfiguration.kmerSize)) {
				//Sketch resultSketch = new Sketch();

				if (currentEnd > inputSequence.getData().length()) {
					currentEnd = inputSequence.getData().length();
				}

				if ((currentEnd - currentStart) >= MCSConfiguration.kmerSize) {

					currentWindow = inputSequence.getData().substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

					// We compute the k-mers. In C
					int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

					//LOG.warn("[JMAbuin] CurrentWindow sketch size: "+sketchValues.length);

					for (int newValue : sketchValues) {

						//this.insert(returnedValues,newValue, this.sequencesIndexes.get(inputSequence.getIdentifier()), numWindows);

						List<LocationBasic> currentList = map.get(newValue);
						if((map.containsKey(newValue)) && (map.get(newValue).size() < 256)) {
							map.get(newValue).add(new LocationBasic(this.sequencesIndexes.get(inputSequence.getIdentifier()), numWindows));
						}
						else if(!map.containsKey(newValue)){

							List<LocationBasic> newList = new ArrayList<LocationBasic>();
							newList.add(new LocationBasic(this.sequencesIndexes.get(inputSequence.getIdentifier()), numWindows));

							map.put(newValue, newList);

						}

					}

				}
				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}
			//LOG.warn("[JMAbuin] Total windows: "+numWindows);

		}

		endTime = System.nanoTime();
		LOG.warn("Time for partition "+ partitionId+" is: " + ((endTime - initTime)/1e9));


		return returnedValues.iterator();
	}


	public void insert(ArrayList<Tuple2<Integer, List<LocationBasic>>> returnedValues, int key, int id, int window) {

		int currentMapSize = returnedValues.size();
		int pos = Math.abs(key) % currentMapSize; // H2 function

		if(returnedValues.get(pos)._1() == -1) { // Empty position. Add current key and value
			List<LocationBasic> newList = new ArrayList<LocationBasic>();
			newList.add(new LocationBasic(id, window));

			returnedValues.set(pos, new Tuple2<Integer, List<LocationBasic>>(key, newList));
		}
		else if(returnedValues.get(pos)._1() == key) { // Busy position with same key. Append value if possible ( vals < 256)
			if(returnedValues.get(pos)._2().size() < 256) {
				returnedValues.get(pos)._2().add(new LocationBasic(id, window));
			}

		}
		else { // Our position is occupied with another key. Iterate until empty or increase ArrayList

			int i;

			for(i = pos; i< returnedValues.size(); i++) {
				if(returnedValues.get(i)._1() == -1) { // Found new empty bucket. Insert there
					List<LocationBasic> newList = new ArrayList<LocationBasic>();
					newList.add(new LocationBasic(id, window));

					returnedValues.set(i, new Tuple2<Integer, List<LocationBasic>>(key, newList));

					break;
				}
				/*else if (returnedValues.get(i)._1() == key) { // Maybe we find our key in another position from  a previous insert
					if(returnedValues.get(pos)._2().size() < 256) {
						returnedValues.get(pos)._2().add(new LocationBasic(id, window));
					}
					break;
				}*/
			}

			if(i == returnedValues.size()) { // Reached end of ArrayList. Increase its size and insert element in first empty position

				int previousSize = returnedValues.size();
				int newSize = (int)(previousSize * 1.5);

				for(int j = previousSize; j < newSize; j++) {
					returnedValues.add(new Tuple2<Integer, List<LocationBasic>>(-1, null));
				}

				List<LocationBasic> newList = new ArrayList<LocationBasic>();
				newList.add(new LocationBasic(id, window));

				returnedValues.set(previousSize, new Tuple2<Integer, List<LocationBasic>>(key, newList));


			}

		}

	}



}
