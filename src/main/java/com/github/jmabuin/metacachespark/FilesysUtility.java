package com.github.jmabuin.metacachespark;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.io.File;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by chema on 1/13/17.
 */
public class FilesysUtility implements Serializable {

	private static final Log LOG = LogFactory.getLog(FilesysUtility.class);


	public static ArrayList<String> files_in_directory(String directory, int recursion_level) {
/*
		File folder = new File(directory);

		LOG.warn("Loading files in: "+directory);

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

		try {
			//JavaSparkContext javaSparkContext = new JavaSparkContext(sparkS.sparkContext());
			Configuration conf = new Configuration();
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
					List<String> newValues = FilesysUtility.files_in_directory(directory + "/" +newFile.getPath().toString(),
							recursion_level + 1);

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
}
