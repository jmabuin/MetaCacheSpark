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

package com.github.jmabuin.metacachespark;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by chema on 1/13/17.
 */
public class FilesysUtility implements Serializable {

	private static final Log LOG = LogFactory.getLog(FilesysUtility.class);


	public static ArrayList<String> files_in_directory(String directory, int recursion_level, JavaSparkContext jsc) {

		try {

		    Configuration conf;

			if (jsc == null) {
				conf = new Configuration();
			}
			else {
			    conf = jsc.hadoopConfiguration();
            }

			FileSystem fs = FileSystem.get(conf);

			ArrayList<String> returnedItems = new ArrayList<String>();

			if(directory == ""){
				directory = fs.getHomeDirectory().toString();

			}
			//System.err.println("[JMAbuin] the current path is: " + path);
			RemoteIterator<LocatedFileStatus> filesInPath = fs.listFiles(new Path(directory), true);

			while(filesInPath.hasNext()) {
				LocatedFileStatus newFile = filesInPath.next();

				//System.err.println("[JMAbuin] found file: " + newFile.getPath().toString());

				if(fs.isFile(newFile.getPath())) {
					//System.err.println("[JMAbuin] Added file: " + newFile.getPath().toString());
					returnedItems.add(newFile.getPath().toString());
				}
				else if(fs.isDirectory(newFile.getPath()) && (recursion_level < 10)) {
					List<String> newValues = FilesysUtility.files_in_directory(directory + "/" + newFile.getPath().toString(),
							recursion_level + 1, jsc);

					if(newValues != null) {
						returnedItems.addAll(newValues);
					}


				}

			}

			//fs.close();

			return returnedItems;

		}
		catch (IOException e) {
			LOG.error("I/O Error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}
		catch (Exception e) {
			LOG.error("General error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}


		return null;


	}

	/*
	 * @brief Find a file in HDFS
	 * @param path The path where to look for the file
	 * @param fileName The file name to find
	 * @return A string containing the full path of the found file or an empty String if it has not been found
	 */
	public static ArrayList<String> findInHDFS(String path, String fileName, JavaSparkContext jsc) {

		try {
			//JavaSparkContext javaSparkContext = new JavaSparkContext(sparkS.sparkContext());
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
			ArrayList<String> returnedItems = new ArrayList<String>();

			if(path == ""){
				path = fs.getHomeDirectory().toString();

			}
			//System.err.println("[JMAbuin] the current path is: " + path);
			RemoteIterator<LocatedFileStatus> filesInPath = fs.listFiles(new Path(path), true);

			while(filesInPath.hasNext()) {
				LocatedFileStatus newFile = filesInPath.next();

				//System.err.println("[JMAbuin] found file: " + newFile.getPath().toString());

				if(newFile.getPath().getName().contains(fileName) && fs.isFile(newFile.getPath())) {
					//System.err.println("[JMAbuin] Added file: " + newFile.getPath().toString());
					returnedItems.add(newFile.getPath().toString());
				}

			}

			//fs.close();

			return returnedItems;

		}
		catch (IOException e) {
			LOG.error("I/O Error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}
		catch (Exception e) {
			LOG.error("General error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}


		return null;

	}

	/*
	 * @brief Find a file in HDFS
	 * @param fileName The file name to find
	 * @return A string containing the full path of the found file or an empty String if it has not been found
	 */
	public static ArrayList<String> findInHDFS(String fileName, JavaSparkContext jsc) {

		return findInHDFS("", fileName, jsc);

	}


	public static ArrayList<String> directories_in_directory_hdfs(String directory, JavaSparkContext jsc) {

		try {
			//JavaSparkContext javaSparkContext = new JavaSparkContext(sparkS.sparkContext());
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
			ArrayList<String> returnedItems = new ArrayList<String>();

			if(directory == ""){
				directory = fs.getHomeDirectory().toString();

			}
			//System.err.println("[JMAbuin] the current path is: " + path);
			RemoteIterator<LocatedFileStatus> filesInPath = fs.listLocatedStatus(new Path(directory));

			while(filesInPath.hasNext()) {
				LocatedFileStatus newFile = filesInPath.next();
				//System.err.println("[JMAbuin] found file: " + newFile.getPath().toString());
				if(fs.isDirectory(newFile.getPath())) {
					returnedItems.add(newFile.getPath().toString());
				}


			}

			return returnedItems;

		}
		catch (IOException e) {
			LOG.error("I/O Error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}
		catch (Exception e) {
			LOG.error("General error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}


		return null;



		/*
		File folder = new File(directory);

		ArrayList<String> returnedFiles = new ArrayList<String>();

		if(folder.isDirectory()) {

			File[] listOfFiles = folder.listFiles();

			for (File currentFile: listOfFiles) {
				if (currentFile.isFile()) {
					returnedFiles.add(directory + "/" +currentFile.getName());
				} else if (currentFile.isDirectory() && recursion_level < 10) {
					returnedFiles.addAll(FilesysUtility.files_in_directory(directory + "/" +currentFile.getName(), recursion_level + 1));
				}
			}
		}

		return returnedFiles;
*/
	}


	public static long readsInFastaFile(String fileName) {

		long numReads = 0L;

		try{
			FileSystem fs = FileSystem.get(new Configuration());

			FSDataInputStream inputStream = fs.open(new Path(fileName));

			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));


			String line;

			while((line = br.readLine()) != null ) {
				if(line.startsWith(">")) {
					numReads++;
				}
			}

			LOG.warn("The number of sequences in the file is: "+numReads);

			br.close();
			inputStream.close();

			return numReads;



		}
		catch(IOException e) {

			LOG.error("Error in FilesysUtility: "+e.getMessage());
			System.exit(-1);
		}

		return numReads;

	}


	public static long readsInFastqFile(String fileName) {

		long numReads = 0L;
		long numLines = 0L;

		try{
			FileSystem fs = FileSystem.get(new Configuration());

			FSDataInputStream inputStream = fs.open(new Path(fileName));

			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));


			String line;

			while( br.readLine() != null ) {

				numLines++;

			}

			br.close();
			inputStream.close();

			if((numLines % 4) != 0) {
				LOG.error("Bad format in fastq file");
				System.exit(1);

			}

			numReads = numLines / 4;


			LOG.warn("The number of sequences in the file is: "+numReads);



			return numReads;



		}
		catch(IOException e) {

			LOG.error("Error in FilesysUtility: "+e.getMessage());
			System.exit(-1);
		}

		return numReads;

	}







	public static boolean isFastaFile(String file_name) {

		if ((file_name.endsWith(".fq")) || (file_name.endsWith(".fastq")) || (file_name.endsWith("fnq"))) {
			return false;
		}

		else if ((file_name.endsWith(".fa")) || (file_name.endsWith(".fasta")) || (file_name.endsWith("fna"))) {
			return true;
		}

		return false;

	}

	public static boolean isFastqFile(String file_name) {

		if ((file_name.endsWith(".fq")) || (file_name.endsWith(".fastq")) || (file_name.endsWith("fnq"))) {
			return true;
		}

		else if ((file_name.endsWith(".fa")) || (file_name.endsWith(".fasta")) || (file_name.endsWith("fna"))) {
			return false;
		}

		return false;

	}

}
