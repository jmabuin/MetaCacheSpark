package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by chema on 1/16/17.
 */
//public class Sketcher2Pair implements FlatMapFunction<Iterator<Sequence>,HashMap<Integer, ArrayList<LocationBasic>>> {
public class Sketcher2Pair implements PairFlatMapFunction<Sequence,Integer, LocationBasic> {
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

	private HashMap<String, Integer> sequencesIndexes;

	public Sketcher2Pair(HashMap<String, Integer> sequencesIndexes) {
		super();
		this.sequencesIndexes = sequencesIndexes;
	}

	@Override
	public Iterable<Tuple2<Integer, LocationBasic>> call(Sequence inputSequence){

	//public Iterable<Integer,Location> call(Sequence inputSequence) {

		ArrayList<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();

		long initTime = System.nanoTime();
		long endTime;

		//while(inputSequences.hasNext()){
		//for(Sequence inputSequence: inputSequences) {

			//Sequence inputSequence = inputSequences.next();

			int currentStart = 0;
			int currentEnd = MCSConfiguration.windowSize;

			String currentWindow = "";
			int numWindows = 0;

			String kmer;
			String reversed_kmer;
			int kmer32;


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

					// Compute k-mers
					kmer = "";
					kmer32 = 0;

					// We compute the k-mers. In C
					int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

					//LOG.warn("[JMAbuin] CurrentWindow sketch size: "+sketchValues.length);

					for (int newValue : sketchValues) {

						returnedValues.add(new Tuple2<Integer, LocationBasic>(newValue,
								new LocationBasic(this.sequencesIndexes.get(inputSequence.getIdentifier()), numWindows)));

					}


				}
				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}
			LOG.warn("[JMAbuin] Total windows: "+numWindows);

		//}

		endTime = System.nanoTime();
		LOG.warn("Time for this partition is: " + ((endTime - initTime)/1e9));


		return returnedValues;
	}





}
