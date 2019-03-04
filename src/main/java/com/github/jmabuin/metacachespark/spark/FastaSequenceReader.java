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

import com.github.jmabuin.metacachespark.Build;
import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Class to read FASTA files from HDFS and store results directly in a RDD of Sequence objects
 */
public class FastaSequenceReader extends SequenceReader implements FlatMapFunction<Tuple2<String, String>, Sequence> {

	private TreeMap<String, Long> sequ2taxid;
	private Build.build_info infoMode;

	private static final Log LOG = LogFactory.getLog(FastaSequenceReader.class);
	private int currentMaxValue = Integer.MAX_VALUE;

	//private HashMap<String, Integer> sequenceIndex;

	//public FastaSequenceReader(HashMap<String, Long> sequ2taxid, Build.build_info infoMode, HashMap<String, Integer> sequenceIndex){
	public FastaSequenceReader(TreeMap<String, Long> sequ2taxid, Build.build_info infoMode){
		//LOG.warn("[JMAbuin] Creating FastaSequenceReader object ");
		super();
		this.sequ2taxid = sequ2taxid;
		this.infoMode = infoMode;

		//this.sequenceIndex = sequenceIndex;
	}

	@Override
	public Iterator<Sequence> call(Tuple2<String, String> arg0) {
		StringBuffer header = new StringBuffer();
		StringBuffer data = new StringBuffer();

		StringBuffer currentInput = new StringBuffer();
		StringBuffer currentFile = new StringBuffer();

		int fileId = 0;

		ArrayList<Sequence> returnedValues = new ArrayList<Sequence>();

		Tuple2<String, String> currentItem;

		//while(arg0.hasNext()) {

			currentInput.delete(0, currentInput.length());
			currentFile.delete(0, currentFile.length());

			//currentItem = arg0.next();
			currentInput.append(arg0._2());
			currentFile.append(arg0._1());

			if(!currentInput.toString().startsWith(">")) {
				return returnedValues.iterator();
			}

			long sequence_number = 0;

			for (String newLine : currentInput.toString().split("\n")) {

				if (newLine.startsWith(">")) {

					if(!header.toString().isEmpty()) {
						//returnedValues.add(new Sequence(data.toString(), "", currentFile.toString(), -1,
						//		header.toString(), -1));
						returnedValues.add(new Sequence(sequence_number, header.toString(), data.toString(), ""));
                        sequence_number++;
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
                returnedValues.add(new Sequence(sequence_number, header.toString(), data.toString(), ""));

			}

			int currentIndexNumber = 0;

			for (Sequence currentSequence : returnedValues) {
				//LOG.info("Processing file: "+ currentFile);


				String seqId = SequenceReader.extract_sequence_id(currentSequence.getHeader());
				String fileIdentifier = SequenceReader.extract_sequence_id(currentFile.toString());

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
				currentSequence.getSequenceOrigin().setIndex(currentIndexNumber);
				currentSequence.getSequenceOrigin().setFilename(currentFile.toString());
				currentSequence.setSeqId(seqId);

				currentIndexNumber++;
			}


		//}

		return returnedValues.iterator();
	}


}