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
package com.github.metacachespark;

//import org.apache.spark.api.java.function.Function2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;


public class Fasta2Features implements FlatMapFunction<Tuple2<String, String>,Feature> {

	private HashMap<String, Long> sequ2taxid;
	private Build.build_info infoMode;

	private static final Log LOG = LogFactory.getLog(Fasta2Features.class);
	private int currentMaxValue = Integer.MAX_VALUE;

	public Fasta2Features(HashMap<String, Long> sequ2taxid, Build.build_info infoMode){
		//LOG.warn("[JMAbuin] Creating FastaSequenceReader object ");
		this.sequ2taxid = sequ2taxid;
		this.infoMode = infoMode;
	}

	@Override
	public Iterable<Feature> call(Tuple2<String, String> arg0) {
		//LOG.warn("[JMAbuin] Starting Call function");
		String header = "";
		String data = "";

		String currentInput = arg0._2();
		String currentFile = arg0._1();

		long fileId = 0;
		ArrayList<Feature> returnedValues = new ArrayList<Feature>();
		ArrayList<Sequence> sequences = new ArrayList<Sequence>();
		boolean isFastaFile;

		long currentLine = 0;
		//Tuple2<String, String> currentInputData;
		long initTime = System.nanoTime();
		long endTime;


		//currentInputData = arg0._2();

		if(!currentInput.startsWith(">")) {
			return returnedValues;
		}

		currentLine = 0;
		isFastaFile = false;
		//sequences.clear();


		for (String newLine : currentInput.split("\n")) {

			if (newLine.startsWith(">")) {

				if(!header.isEmpty()) {
					sequences.add(new Sequence(data, 0, fileId, currentFile, header, -1));
				}

				header = newLine.substring(1);
				data = "";
			}
			else {

				data = data + newLine;

			}

			currentLine++;

		}

		if ((!data.isEmpty()) && (!header.isEmpty())) {
			sequences.add(new Sequence(data, 0, fileId, currentFile, header, -1));

		}

		//if(isFastaFile) {

		for (Sequence currentSequence : sequences) {
			//LOG.info("Processing file: "+ currentFile);

			String seqId = SequenceReader.extract_sequence_id(currentSequence.getHeader());
			String fileIdentifier = SequenceReader.extract_sequence_id(currentSequence.getCurrentFile());

			//make sure sequence id is not empty,
			//use entire header if neccessary
			if (seqId.isEmpty()) {
				if (!fileIdentifier.isEmpty()) {
					seqId = fileIdentifier;
				} else {
					seqId = currentSequence.getHeader();
				}
			}

			//targets need to have a sequence id
			//look up taxon id
			int taxid = 0;

			if (!sequ2taxid.isEmpty()) {
				Long it = sequ2taxid.get(seqId);
				if (it != null) {
					taxid = it.intValue();
				} else {
					it = sequ2taxid.get(fileIdentifier);
					if (it != null) {
						taxid = it.intValue();
					}
				}
			}
			//no valid taxid assigned -> try to find one in annotation
			if (taxid > 0) {
				if (infoMode == Build.build_info.verbose)
					LOG.info("[" + seqId + ":" + taxid + "] ");
			} else {
				taxid = SequenceReader.extract_taxon_id(currentSequence.getHeader()).intValue();
				if (infoMode == Build.build_info.verbose)
					LOG.info("[" + seqId + "] ");
			}

			currentSequence.setTaxid(taxid);


			int currentStart = 0;
			int currentEnd = MCSConfiguration.windowSize;



			//ArrayList<Feature> returnedValues = new ArrayList<Feature>();

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
					//resultSketch.insert(new Feature(newValue,
					//		partitionId, fileId, header, taxid));

					returnedValues.add(new Feature(newValue, currentSequence.getPartitionId(), currentSequence.getFileId(), currentSequence.getHeader(), currentSequence.getTaxid()));

				}

				// We compute the k-mers


				//returnedValuesS.add(resultSketch);


				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}





		}
		endTime = System.nanoTime();
		LOG.warn("Time for file "+currentFile+" is: " + ((endTime - initTime)/1e9));
		return returnedValues;
	}

}
