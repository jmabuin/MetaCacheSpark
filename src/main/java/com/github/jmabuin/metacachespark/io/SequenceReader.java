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

package com.github.jmabuin.metacachespark.io;

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
import java.io.Serializable;

/**
 * Base class to read a sequence file (FASTA or FASTQ) sequentially from HDFS.
 * @author Jose M. Abuin
 */


public class SequenceReader implements Serializable{

	private static final Log LOG = LogFactory.getLog(SequenceReader.class); // LOG to show messages

	private String 							inputFile;		// File where the sequences are stored
	private JavaSparkContext 				jsc;			// JavaSparkContext object to use
	private StringBuffer 					bufferHeader;	// Buffer to store sequence headers
	private StringBuffer 					bufferData;		// Buffer to store sequence data
	private StringBuffer 					bufferQuality;	// Buffer to store sequence quality (in case of FASTQ)

	private BufferedReader 					br;				// BufferedReader to read input file
	private FSDataInputStream 				inputStream;	// InputStream to read input file
	private MetaCacheOptions.InputFormat 	currentFormat;	// File format, FASTQ or FASTA


	/**
	 * @brief Basic builder
	 */
	public SequenceReader(){

	}

	/**
	 * @brief Builder when considering a file and a spark context
	 * @param fileName The name of the file where the sequences are stored
	 * @param jsc The JavaSparContext we are using
	 */
	public SequenceReader(String fileName, JavaSparkContext jsc) {

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
	 * From original MetaCache
	 */
	private static String[] accession_prefix =
	{
		"NC_", "NG_", "NS_", "NT_", "NW_", "NZ_", "AC_", "GCF_",
		"AE", "AJ", "AL", "AM", "AP", "AY",
		"BA", "BK", "BX",
		"CP", "CR", "CT", "CU",
		"FM", "FN", "FO", "FP", "FQ", "FR",
		"HE", "JH"
	};

	/**
	 * From original MetaCache
	 */
	public static String extract_sequence_id(String text) {
		String sequid = extract_ncbi_accession_version_number(text);
		if(!sequid.isEmpty()) {
			return sequid;
		}

		sequid = extract_genbank_identifier(text);
		if(!sequid.isEmpty()) {
			return sequid;
		}

		return extract_ncbi_accession_number(text);
	}

	/**
	 * From original MetaCache
	 */
	public static String extract_ncbi_accession_number(String text) {

		for(String prefix : accession_prefix) {
			String num = extract_ncbi_accession_number(prefix, text);

			if(!num.isEmpty()) {
				return num;
			}

		}
		return "";
	}

	/**
	 * From original MetaCache
	 */
	public static String extract_ncbi_accession_number(String prefix, String text) {

		if(text.contains(prefix)) {
			int i = text.indexOf(prefix);
			int j = i + prefix.length();

			int k = text.indexOf("|", j);

			if(k == -1){
				k = text.indexOf(" ", j);
				if(k == -1) {
					k = text.indexOf(".", j);
					if (k == -1) {
						k = text.indexOf("-", j);
						if (k == -1) {
							k = text.indexOf("_", j);
							if (k == -1) {
								k = text.indexOf(",", j);
								if (k == -1) {
									k = text.length();
								}

							}
						}
					}
				}
			}

			return text.substring(i, k);

		}

		return "";

	}

	/**
	 * From original MetaCache
	 */
	public static String extract_ncbi_accession_version_number(String prefix, String text) {

		int i = text.indexOf(prefix);

		if(i != -1) {
			// find version separator
			int j = text.indexOf(".", i+prefix.length());

			if(j == -1) {
				return "";
			}

			//find end of accession.version string
			int k = text.indexOf("|", j);

			if(k == -1){
				k = text.indexOf(" ", j);
				if(k == -1) {
					k = text.indexOf("-", j);
					if (k == -1) {
						k = text.indexOf("_", j);
						if (k == -1) {
							k = text.indexOf(",", j);
							if (k == -1) {
								k = text.length();
							}

						}
					}
				}
			}
			//System.err.println("[JMAbuin] i is "+i+" and k is "+k);
			return text.substring(i, k);

		}

		return "";

	}

	/**
	 * From original MetaCache
	 */
	public static String extract_ncbi_accession_version_number(String text) {

		for(String prefix : accession_prefix) {
			String num = extract_ncbi_accession_version_number(prefix, text);
			if(!num.isEmpty()) return num;
		}

		return "";
	}

	/**
	 * From original MetaCache
	 */
	public static String extract_genbank_identifier(String text) {

		int i = text.indexOf("gi|");

		if(i != -1) {
			//skip prefix
			i += 3;

			//find end of number
			int j = text.indexOf('|', i);

			if(j == -1) {
				j = text.indexOf(' ', i);

				if(j == -1) {
					j = text.length();
				}
			}

			return text.substring(i, j);
		}

		return "";
	}

	/**
	 * From original MetaCache
	 */
	public static Long extract_taxon_id(String text) {
		int i = text.indexOf("taxid");

		if(i != -1) {
			//skip "taxid" + separator char
			i += 6;
			//find end of number
			int j = text.indexOf('|', i);

			if(j == -1) {
				j = text.indexOf(' ', i);
				if(j == -1) {
					j = text.length();
				}
			}

			try {
				return Long.parseLong(text.substring(i, j));
			}
			catch(Exception e) {
				return 0L;
			}
		}
		return 0L;
	}


	/**
	 * This function build the next sequence in the file and returns it as a new SequenceData
	 * @return The new SequenceData object that represents the sequence read
	 */
	public SequenceData next() {

		if(this.currentFormat == MetaCacheOptions.InputFormat.FASTA) {

			// Get a new FASTA record from file
			try {
				// We read lines until we found a new header or end of file
				for(String line; (line = this.br.readLine()) != null; ) {

					// Case of first header in file
					if((line.startsWith(">")) && (this.bufferHeader.toString().isEmpty())) {
						this.bufferHeader.append(line.subSequence(1,line.length()));
					}
					// Case of new header found after a new sequence data. We build the new SequenceData to return and store the new header
					else if ((line.startsWith(">")) && (!this.bufferHeader.toString().isEmpty())) {
						// New sequence found. Create new record, delete old header, save new header and return record
						SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),"");
						this.bufferHeader.delete(0, this.bufferHeader.length());
						this.bufferData.delete(0, this.bufferData.length());

						this.bufferHeader.append(line.subSequence(1,line.length()));

						return currentSequenceData;

					}
					// Case of new line with data
					else {
						this.bufferData.append(line.replace("\n", ""));
					}
				}

				// At the end, if we don't have data, is because we are at the end of the file. Return null
				if(this.bufferData.toString().isEmpty() && (this.bufferHeader.toString().isEmpty())) {
					return null;
				}

				// If we have data, build the new record and return it.
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

}
