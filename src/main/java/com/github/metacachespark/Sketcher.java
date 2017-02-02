package com.github.metacachespark;


import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by chema on 1/16/17.
 */
public class Sketcher implements Function<Sequence,Iterator<Sketch>> {


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


	public int kmer2int32(String inputKmer) {

		char[] characters = inputKmer.toCharArray();

		int returnedValue = 0x00000000;

		for(char character: characters) {

			returnedValue = returnedValue  << 2;

			switch(character) {
				case 'A': case 'a': break;
				case 'C': case 'c': returnedValue |= 0x0000001; break;
				case 'G': case 'g': returnedValue |= 0x0000002; break;
				case 'T': case 't': returnedValue |= 0x0000003; break;
				default: break;
			}


		}

		return returnedValue;

	}

	@Override
	public Iterator<Sketch> call(Sequence inputSequence) {

		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		String data = inputSequence.getData();

		ArrayList<Sketch> returnedValues = new ArrayList<Sketch>();

		String currentWindow = "";
		int numWindows = 0;

		String kmer;
		String reversed_kmer;
		int kmer32;



		// We iterate over windows (with overlap)
		while (currentEnd < data.length()) {
			Sketch resultSketch = new Sketch();

			currentWindow = data.substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

			// Compute k-mers
			kmer = "";
			kmer32 = 0;


			// We compute the k-mers
			for (int i = 0; i < currentWindow.length() - MCSConfiguration.kmerSize; i++) {

				kmer = currentWindow.substring(i, i + MCSConfiguration.kmerSize);
				//reversed_kmer = HashFunctions.reverse_complement(kmer);

				kmer32 = HashFunctions.kmer2uint32(kmer);//this.kmer2int32(kmer);

				// Apply hash to current kmer
				int hashValue = HashFunctions.make_canonical(this.hash_(kmer32), MCSConfiguration.kmerSize);

				resultSketch.insert(new Feature(hashValue,
						inputSequence.getPartitionId(),
						inputSequence.getFileId(),
						inputSequence.getHeader(),
						inputSequence.getTaxid()));

			}

			returnedValues.add(resultSketch);


			numWindows++;
			currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
			currentEnd = currentStart + MCSConfiguration.windowSize;

		}

		return returnedValues.iterator();

	}


}
