package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by chema on 2/17/17.
 */
public class SequenceFileReader {

	private static final Log LOG = LogFactory.getLog(SequenceFileReader.class); // LOG to show messages

	private String 							inputFile;		// File where the sequences are stored
	private JavaSparkContext jsc;			// JavaSparkContext object to use
	private StringBuffer 					bufferHeader;	// Buffer to store sequence headers
	private StringBuffer 					bufferData;		// Buffer to store sequence data
	private StringBuffer 					bufferQuality;	// Buffer to store sequence quality (in case of FASTQ)

	private BufferedReader br;				// BufferedReader to read input file
	private FSDataInputStream inputStream;	// InputStream to read input file
	private MetaCacheOptions.InputFormat 	currentFormat;	// File format, FASTQ or FASTA

	/**
	 * @brief Basic builder
	 */
	public SequenceFileReader(){

	}

	/**
	 * @brief Builder when considering a file and a spark context
	 * @param fileName The name of the file where the sequences are stored
	 * @param jsc The JavaSparContext we are using
	 */
	public SequenceFileReader(String fileName, JavaSparkContext jsc) {

		// Variable initialization
		this.inputFile = fileName;
		this.jsc = jsc;

		this.bufferData = new StringBuffer();
		this.bufferHeader = new StringBuffer();
		this.bufferQuality = new StringBuffer();

		// Try to open the filesystem (HDFS) and sequence file
		try {
			FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			this.inputStream = fs.open(new Path(this.inputFile));

			this.br = new BufferedReader(new InputStreamReader(inputStream));

			// Obtain the sequences file format
			if (this.inputFile.endsWith(".fastq") || this.inputFile.endsWith(".fq") || this.inputFile.endsWith(".fnq")) {
				this.currentFormat = MetaCacheOptions.InputFormat.FASTQ;
			}
			else {
				this.currentFormat = MetaCacheOptions.InputFormat.FASTA;
			}

		}
		catch (IOException e) {
			LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.");
			e.printStackTrace();
			//System.exit(1);
		}
		catch (Exception e) {
			LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.");
			e.printStackTrace();
			//System.exit(1);
		}



	}

	/**
	 * This function build the next sequence in the file and returns it as a new SequenceData
	 * @return The new SequenceData object that represents the sequence read
	 */
	public SequenceData next() {
		//LOG.warn("[JMAbuin] Call to next");
		if(this.currentFormat == MetaCacheOptions.InputFormat.FASTA) {

			// Get a new FASTA record from file
			try {
				// We read lines until we found a new header or end of file
				//LOG.warn("[JMAbuin] Processing next sequence");
				for(String line; (line = this.br.readLine()) != null; ) {

					// Case of first header in file
					if((line.startsWith(">")) && (this.bufferHeader.toString().isEmpty())) {
						this.bufferHeader.append(line.subSequence(1,line.length()));
						//LOG.warn("[JMAbuin] Header");
					}
					// Case of new header found after a new sequence data. We build the new SequenceData to return and store the new header
					else if ((line.startsWith(">")) && (!this.bufferHeader.toString().isEmpty())) {
						// New sequence found. Create new record, delete old header, save new header and return record
						SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),"");
						this.bufferHeader.delete(0, this.bufferHeader.length());
						this.bufferData.delete(0, this.bufferData.length());

						this.bufferHeader.append(line.subSequence(1,line.length()));
						//LOG.warn("[JMAbuin] New header and return");
						return currentSequenceData;

					}
					// Case of new line with data
					else {
						this.bufferData.append(line.replace("\n", ""));
						//LOG.warn("[JMAbuin] Data");
					}
				}

				// At the end, if we don't have data, is because we are at the end of the file. Return null
				if(this.bufferData.toString().isEmpty() && (this.bufferHeader.toString().isEmpty())) {
					return null;
				}

				// If we have data, build the last record and return it.
				SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),"");

				this.bufferHeader.delete(0, this.bufferHeader.length());
				this.bufferData.delete(0, this.bufferData.length());

				return currentSequenceData;
			}
			catch (IOException e) {
				LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
				e.printStackTrace();
				//System.exit(1);
			}
			catch (Exception e) {
				LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
				e.printStackTrace();
				//System.exit(1);
			}

		}
		else if (this.currentFormat == MetaCacheOptions.InputFormat.FASTQ) {

			// Get a new FASTQ record from file
			try {

				int i = 0;

				for(String line; ((line = this.br.readLine()) != null) && (i < 4); i++) {

					if (i == 0) {
						this.bufferHeader.append(line);
					}
					else if (i == 1) {
						this.bufferData.append(line);
					}
					else if (i == 3) {
						this.bufferQuality.append(line);
					}


				}

				if(this.bufferData.toString().isEmpty() && (this.bufferHeader.toString().isEmpty())) {
					return null;
				}

				SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),this.bufferQuality.toString());

				this.bufferHeader.delete(0, this.bufferHeader.length());
				this.bufferData.delete(0, this.bufferData.length());
				this.bufferQuality.delete(0, this.bufferQuality.length());

				return currentSequenceData;
			}
			catch (IOException e) {
				LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
				e.printStackTrace();
				//System.exit(1);
			}
			catch (Exception e) {
				LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
				e.printStackTrace();
				//System.exit(1);
			}
		}

		return null;

	}

	/**
	 * Closes the buffers readers
	 */
	public void close(){
		try {
			this.br.close();
			this.inputStream.close();
		}
		catch (IOException e) {
			LOG.error("Error closing "+ this.inputFile+ " because of IO error in SequenceReader.close().");
			e.printStackTrace();
			//System.exit(1);
		}
		catch (Exception e) {
			LOG.error("Could closing file "+ this.inputFile+ " because of error in SequenceReader.close().");
			e.printStackTrace();
			//System.exit(1);
		}

	}

	public ArrayList<Sketch> getSketch(SequenceData sequence) {


		int currentStart = 0;
		int currentEnd = MCSConfiguration.windowSize;

		ArrayList<Sketch> returnedValues = new ArrayList<Sketch>();
		//ArrayList<Location> returnedValues = new ArrayList<Location>();

		String currentWindow = "";
		int numWindows = 0;


		// We iterate over windows (with overlap)
		//while (currentEnd < sequence.getData().length()) {
		while (currentStart < (sequence.getData().length() - MCSConfiguration.kmerSize)) {
			//Sketch resultSketch = new Sketch();
			if(currentEnd > sequence.getData().length()) {
				currentEnd = sequence.getData().length();
			}

			//LOG.warn("[JMAbuin] Init: " + currentStart+" - End: "+currentEnd);

			currentWindow = sequence.getData().substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

			// Compute k-mers
			// We compute the k-mers. In C
			int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

			//for(int newValue: sketchValues) {

				//returnedValues.add(new Location(newValue, 0, numWindows));
				returnedValues.add(new Sketch(sequence.getHeader(), sequence.getData(), sketchValues));

			//}

			// We compute the k-mers


			//returnedValuesS.add(resultSketch);


			numWindows++;
			currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
			currentEnd = currentStart + MCSConfiguration.windowSize;




		}


		return returnedValues;
	}

}
