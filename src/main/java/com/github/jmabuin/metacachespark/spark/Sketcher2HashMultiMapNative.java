package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by chema on 3/16/17.
 */
public class Sketcher2HashMultiMapNative implements Function2<Integer, Iterator<Sequence> ,Iterator<HashMultiMapNative>> {

	private static final Log LOG = LogFactory.getLog(Sketcher2HashMultiMapNative.class);
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

	public Sketcher2HashMultiMapNative(HashMap<String, Integer> sequencesIndexes) {

		this.sequencesIndexes = sequencesIndexes;
	}

	@Override
	public Iterator<HashMultiMapNative> call(Integer partitionId, Iterator<Sequence> inputSequences){

		//public Iterable<Integer,Location> call(Sequence inputSequence) {

		LOG.warn("Starting to process partition: "+partitionId);

		int initialSize = 5;

		ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();

		HashMultiMapNative map = new HashMultiMapNative();

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


						map.add(newValue, this.sequencesIndexes.get(inputSequence.getIdentifier()), numWindows);


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

}
