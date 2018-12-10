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
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Created by chema on 1/16/17.
 */
public class Sketcher implements FlatMapFunction<Sequence,Location> {

	private static final Log LOG = LogFactory.getLog(Sketcher.class);
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

	public Sketcher(TreeMap<String, Integer> sequencesIndexes) {
		//super();
		this.sequencesIndexes = sequencesIndexes;
	}

	@Override
	public Iterator<Location> call(Sequence inputSequence) {

		ArrayList<Location> returnedValues = new ArrayList<Location>();

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
						returnedValues.add(new Location(newValue,
								this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows));


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
