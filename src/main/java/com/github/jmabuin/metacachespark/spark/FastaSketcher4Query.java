/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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
import com.github.jmabuin.metacachespark.io.SequenceData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.*;


public class FastaSketcher4Query implements FlatMapFunction<Tuple2<String, String>,Sketch> {


	private static final Log LOG = LogFactory.getLog(FastaSketcher4Query.class);
	//private int currentMaxValue = Integer.MAX_VALUE;


	@Override
	public Iterable<Sketch> call(Tuple2<String, String> arg0) {
		//LOG.warn("[JMAbuin] Starting Call function");
		//String header = "";
		StringBuffer header = new StringBuffer();
		StringBuffer data = new StringBuffer();

		String currentInput = arg0._2();
		String currentFile = arg0._1();

		int fileId = 0;
		ArrayList<Location> locations = new ArrayList<Location>();
		ArrayList<Sketch> returnedValues = new ArrayList<Sketch>();
		ArrayList<Sequence> sequences = new ArrayList<Sequence>();


		if(!currentInput.startsWith(">")) {
			return returnedValues;
		}


		for (String newLine : currentInput.split("\n")) {

			if (newLine.startsWith(">")) {

				if(!header.toString().isEmpty()) {
					sequences.add(new Sequence(data.toString(), 0, fileId, currentFile, header.toString(), -1));
				}

				header.delete(0,header.length());
				header.append(newLine.substring(1));
				//data = "";
				data.delete(0,data.length());
			}
			else {

				//data = data + newLine;
				data.append(newLine);

			}

			//currentLine++;

		}

		if ((!data.toString().isEmpty()) && (!header.toString().isEmpty())) {
			sequences.add(new Sequence(data.toString(), 0, fileId, currentFile, header.toString(), -1));

		}
		//endTime = System.nanoTime();
		//LOG.warn(currentFile+" Time used in build sequence data: "+(endTime-initTime)/1e9);
		//if(isFastaFile) {

		for (Sequence currentSequence : sequences) {
			//LOG.info("Processing file: "+ currentFile);

			int currentStart = 0;
			int currentEnd = MCSConfiguration.windowSize;



			//ArrayList<Location> returnedValues = new ArrayList<Location>();

			String currentWindow = "";
			int numWindows = 0;


			// We iterate over windows (with overlap)
			while (currentEnd < currentSequence.getData().length()) {
				//Sketch resultSketch = new Sketch();

				currentWindow = currentSequence.getData().substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

				// Compute k-mers
				// We compute the k-mers. In C
				int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

				for(int newValue: sketchValues) {

					//returnedValues.add(new Location(newValue, 0, numWindows));
					returnedValues.add(new Sketch(currentSequence.getHeader(), currentSequence.getData(), sketchValues));

				}

				// We compute the k-mers


				//returnedValuesS.add(resultSketch);


				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}

		}


		//endTime = System.nanoTime();
		//LOG.warn("Time for file "+currentFile+" is: " + ((endTime - initTime)/1e9));
		return returnedValues;
	}


}
