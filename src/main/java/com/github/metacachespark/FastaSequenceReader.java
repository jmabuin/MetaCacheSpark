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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

//public class FastaSequenceReader implements PairFlatMapFunction<Iterator<String>, Integer, ArrayList<Integer>> {
/*
public class FastaSequenceReader implements Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Tuple2<Integer, ArrayList<Location>>>> {
	public Iterator<Tuple2<Integer, ArrayList<Location>>> call(Integer partitionId, Iterator<Tuple2<String, String>> arg0) {

		String header = "";
		String data = "";
		String qualities = "";

		String currentInput = "";
		String currentFile = "";

		Hashtable<Integer, ArrayList<Location>> returnedValuesHashTable = new Hashtable<Integer, ArrayList<Location>>();

		ArrayList<Tuple2<Integer, ArrayList<Location>>> returnedValues = new ArrayList<Tuple2<Integer, ArrayList<Location>>>();

		while(arg0.hasNext()) {

			currentInput = arg0.next()._2;
			currentFile = arg0.next()._1;

			for (String newLine : currentInput.split("\n")) {

				if (newLine.startsWith(">")) { // We are in the header
					header = newLine.substring(1);
				} else {
					data = data + newLine;
				}

			}

			int currentStart = 0;
			int currentEnd = MCSConfiguration.windowSize;

			String currentWindow = "";
			int numWindows = 0;

			int[] features = new int[MCSConfiguration.sketchSize];
			String kmer;
			String reversed_kmer;
			int kmer32;

			// We iterate over windows (with overlap)
			while (currentEnd < data.length()) {

				currentWindow = data.substring(currentStart, currentEnd + 1);

				for (int j = 0; j < features.length; j++) {
					features[j] = Integer.MIN_VALUE;
				}

				// Compute k-mers
				kmer = "";
				kmer32 = 0;


				// We compute the k-mers
				for (int i = 0; i < currentWindow.length(); i++) {

					kmer = currentWindow.substring(i, i + MCSConfiguration.kmerSize);
					reversed_kmer = HashFunctions.reverse_complement(kmer);

					kmer32 = this.kmer2int32(kmer);

					// Apply hash to current kmer
					int hashValue = HashFunctions.thomas_mueller_hash(kmer32);
					this.insert(hashValue, features);

					// Apply hash to reverse complement of the kmer
					kmer32 = this.kmer2int32(reversed_kmer);

					// Apply hash to current kmer
					hashValue = HashFunctions.thomas_mueller_hash(kmer32);
					this.insert(hashValue, features);

				}

				// Insert sketch (16 features) into hashtable
				for (int i = 0; i< features.length; i++) {

					if(returnedValuesHashTable.containsKey(features[i])) {
						returnedValuesHashTable.get(features[i]).add(new Location((short)0,numWindows,partitionId, header));
					}
					else {
						ArrayList<Location> newArrayList = new ArrayList<Location>();
						newArrayList.add(new Location((short)0,numWindows,partitionId, header));
						returnedValuesHashTable.put(features[i], newArrayList);

					}
				}

				numWindows++;
				currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
				currentEnd = currentStart + MCSConfiguration.windowSize;

			}

			//ResultType newResult = new ResultType(header, data, qualities);
		}

		Iterator<Map.Entry<Integer, ArrayList<Location>>> valuesInMap = returnedValuesHashTable.entrySet().iterator();

		while(valuesInMap.hasNext()) {
			Map.Entry<Integer, ArrayList<Location>> currentEntry = valuesInMap.next();
			returnedValues.add(new Tuple2<Integer, ArrayList<Location>>(currentEntry.getKey(), currentEntry.getValue()));
		}

		return returnedValues.iterator();
	}
*/
public class FastaSequenceReader implements Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Location>> {

	private HashMap<String, Long> sequ2taxid;
	private Build.build_info infoMode;

	private static final Log LOG = LogFactory.getLog(FastaSequenceReader.class);

	public FastaSequenceReader(HashMap<String, Long> sequ2taxid, Build.build_info infoMode){
		this.sequ2taxid = sequ2taxid;
		this.infoMode = infoMode;
	}

	public Iterator<Location> call(Integer partitionId, Iterator<Tuple2<String, String>> arg0) {

		String header = "";
		String data = "";
		String qualities = "";

		String currentInput = "";
		String currentFile = "";

		long fileId = 0;
		ArrayList<Location> returnedValues = new ArrayList<Location>();

		while(arg0.hasNext()) {

			currentInput = arg0.next()._2;
			currentFile = arg0.next()._1;


			// We could do str.replace("\n",""); ??
			//data = data.replace("\n", "");

			for (String newLine : currentInput.split("\n")) {

				if (newLine.startsWith(">")) { // We are in the header
					header = newLine.substring(1);
				} else {
					data = data + newLine;
				}

			}

			String seqId = SequenceReader.extract_sequence_id(header);
			String fileIdentifier = SequenceReader.extract_sequence_id(currentFile);

			//make sure sequence id is not empty,
			//use entire header if neccessary
			if(seqId.isEmpty()) {
				if(!fileIdentifier.isEmpty()) {
					seqId = fileIdentifier;
				} else {
					seqId = header;
				}
			}

			//targets need to have a sequence id
			//look up taxon id
			int taxid = 0;

			if(!sequ2taxid.isEmpty()) {
				Long it = sequ2taxid.get(seqId);
				if(it != null) {
					taxid = it.intValue();
				} else {
					it = sequ2taxid.get(fileIdentifier);
					if(it != null) {
						taxid = it.intValue();
					}
				}
			}
			//no valid taxid assigned -> try to find one in annotation
			if(taxid > 0) {
				if(infoMode == Build.build_info.verbose)
					 LOG.info("[" + seqId + ":" + taxid + "] ");
			}
			else {
				taxid = SequenceReader.extract_taxon_id(header).intValue();
				if(infoMode == Build.build_info.verbose)
					LOG.info("[" + seqId + "] ");
			}


			//try to add to database
			boolean added = this.add_target(data, partitionId, (short)fileId, currentFile, header, returnedValues);

			if(infoMode == Build.build_info.verbose && !added) {
				LOG.info(seqId + " not added to database");
			}


			fileId++;

			//ResultType newResult = new ResultType(header, data, qualities);
		}


		return returnedValues.iterator();

	}


	public boolean add_target(String data, int partitionId, short fileId, String currentFile, String header, ArrayList<Location> returnedValues) {

		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		String currentWindow = "";
		int numWindows = 0;

		int[] features = new int[MCSConfiguration.sketchSize];
		String kmer;
		String reversed_kmer;
		int kmer32;

		// We iterate over windows (with overlap)
		while (currentEnd < data.length()) {

			currentWindow = data.substring(currentStart, currentEnd + 1);

			for (int j = 0; j < features.length; j++) {
				features[j] = Integer.MIN_VALUE;
			}

			// Compute k-mers
			kmer = "";
			kmer32 = 0;


			// We compute the k-mers
			for (int i = 0; i < currentWindow.length(); i++) {

				kmer = currentWindow.substring(i, i + MCSConfiguration.kmerSize);
				reversed_kmer = HashFunctions.reverse_complement(kmer);

				kmer32 = this.kmer2int32(kmer);

				// Apply hash to current kmer
				int hashValue = HashFunctions.thomas_mueller_hash(kmer32);
				this.insert(hashValue, features);

				// Apply hash to reverse complement of the kmer
				kmer32 = this.kmer2int32(reversed_kmer);

				// Apply hash to current kmer
				hashValue = HashFunctions.thomas_mueller_hash(kmer32);
				this.insert(hashValue, features);

			}

			// Insert sketch (16 features) into hashtable
			for (int i = 0; i< features.length; i++) {

				returnedValues.add(new Location(features[i], fileId, numWindows, partitionId, currentFile, header));


			}

			numWindows++;
			currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
			currentEnd = currentStart + MCSConfiguration.windowSize;

		}

		return true;
	}


	public void insert(int val,int[] arr){
		int i;

		for(i = 0;i < arr.length-1;i++){
			if(arr[i]>val)
				break;
		}

		if(i >= arr.length) {
			return;
		}

		for(int k=arr.length-2; k>=i; k--){
			arr[k+1]=arr[k];
		}
		arr[i]=val;
		//System.out.println(Arrays.toString(arr));

	}

	public long kmer2long64(String kmer) {

		char[] characters = kmer.toCharArray();

		int returnedValue = 0x0000000000000000;

		for(char character: characters) {

			byte newChar = (byte) character;

			returnedValue = (returnedValue | newChar) << 8;

		}

		return returnedValue;

	}

	public int kmer2int32(String inputKmer) {

		char[] characters = inputKmer.toCharArray();

		int returnedValue = 0x00000000;

		for(char character: characters) {


			switch(character) {
				case 'A': case 'a': break;
				case 'C': case 'c': returnedValue |= 0x0000001; break;
				case 'G': case 'g': returnedValue |= 0x0000002; break;
				case 'T': case 't': returnedValue |= 0x0000003; break;
				default: break;
			}

			returnedValue = returnedValue  << 2;

		}

		return returnedValue;

	}


}
