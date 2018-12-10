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

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;

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

		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		ArrayList<Location> returnedValues = new ArrayList<Location>();

		String currentWindow = "";
		int numWindows = 0;

		String kmer;
		String reversed_kmer;
		int kmer32;

		long initTime = System.nanoTime();
		long endTime;

		// We iterate over windows (with overlap)
		//while (currentEnd <= inputSequence.getData().length()) {
		//LOG.warn("[JMAbuin] Processing sequence: "+inputSequence.getSequenceOrigin().getFilename());
		while (currentStart < (inputSequence.getData().length() - MCSConfiguration.kmerSize)) {
			//Sketch resultSketch = new Sketch();

			if(currentEnd > inputSequence.getData().length()) {
				currentEnd = inputSequence.getData().length();
			}

			if((currentEnd- currentStart) >= MCSConfiguration.kmerSize) {

				currentWindow = inputSequence.getData().substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

				// Compute k-mers
				kmer = "";
				kmer32 = 0;

				// We compute the k-mers. In C
				int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

				for (int newValue : sketchValues) {
					//resultSketch.insert(new Location(newValue,
					//		partitionId, fileId, header, taxid));

					//returnedValues.add(new Location(newValue, inputSequence.getTaxid(), numWindows));
					//returnedValues.add(new Location(newValue, this.sequencesIndexes.get(inputSequence.getIdentifier()), numWindows));
					returnedValues.add(new Location(newValue, this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows));

				}

				// We compute the k-mers


				//returnedValuesS.add(resultSketch);

			}
			numWindows++;
			currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
			currentEnd = currentStart + MCSConfiguration.windowSize;


		}


		endTime = System.nanoTime();
		//LOG.warn("Time for file "+inputSequence.getFileName()+" is: " + ((endTime - initTime)/1e9));

		return returnedValues.iterator();
	}





}
