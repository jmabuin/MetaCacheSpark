package com.github.metacachespark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by jabuinmo on 30.01.17.
 */
public class MetaCacheSpark implements Serializable {

	private static final Log LOG = LogFactory.getLog(MetaCacheSpark.class);

	public static void main(String[] args) {

		MetaCacheOptions newOptions = new MetaCacheOptions(args);

		if(newOptions.getMode() == MetaCacheOptions.Mode.HELP) {
			newOptions.printHelp();
		}
		else if(newOptions.getMode() == MetaCacheOptions.Mode.BUILD) {
			// Build mode entry point
			SparkSession sparkS = SparkSession
					.builder()
					.appName("MetaCacheSpark - Build")
					.getOrCreate();

			String buildArgs[] = newOptions.getOtherOptions();

			Build buildObject = new Build(buildArgs, sparkS);
			buildObject.buildDatabase();

			LOG.info("End of program ...");


		}
		else if(newOptions.getMode() == MetaCacheOptions.Mode.QUERY) {
			// Query mode entry point
			SparkSession sparkS = SparkSession
					.builder()
					.appName("MetaCacheSpark - Query")
					.getOrCreate();

			String queryArgs[] = newOptions.getOtherOptions();

			//Build buildObject = new Build(buildArgs, sparkS);
			//buildObject.buildDatabase();

			LOG.info("End of program ...");
		}
		else {
			System.out.println("Not recognized option");
			newOptions.printHelp();
			//System.exit(1);
		}


		//System.exit(0);
	}
}
