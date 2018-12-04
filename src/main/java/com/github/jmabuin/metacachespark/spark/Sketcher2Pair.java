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
import java.util.TreeMap;

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

	private TreeMap<String, Integer> sequencesIndexes;

	public Sketcher2Pair(TreeMap<String, Integer> sequencesIndexes) {
		super();
		this.sequencesIndexes = sequencesIndexes;
	}

	@Override
	public Iterator<Tuple2<Integer, LocationBasic>> call(Sequence inputSequence){

		//public Iterable<Integer,Location> call(Sequence inputSequence) {

		ArrayList<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();

		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		//String currentWindow = "";
		//StringBuffer currentWindow = new StringBuffer();
		int numWindows = 0;
		int current_sketch_size = MCSConfiguration.sketchSize;

		String kmer;
		String reversed_kmer;
		int kmer32;

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

				// Compute k-mers
				kmer = "";
				kmer32 = 0;
				//LOG.warn("[JMAbuin] Current window is: " + currentWindow);
				// We compute the k-mers. In C
				int sketchValues[] = HashFunctions.window2sketch32(inputSequence.getData().substring(currentStart, currentEnd)
						, current_sketch_size, MCSConfiguration.kmerSize);

				if (sketchValues != null) {
					//LOG.warn("[JMAbuin] CurrentWindow sketch size: " + sketchValues.length);

					for (int newValue : sketchValues) {

						//LOG.warn("Calculated value: " + newValue);
						returnedValues.add(new Tuple2<Integer, LocationBasic>(newValue,
								new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows)));


					}
				}


			}
			numWindows++;
			currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
			currentEnd = currentStart + MCSConfiguration.windowSize;

		}
		//LOG.warn("[JMAbuin] Total windows: "+numWindows);

		//}


		return returnedValues.iterator();
	}





}
