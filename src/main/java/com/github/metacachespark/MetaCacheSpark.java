package com.github.metacachespark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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

			SparkConf sparkConf = new SparkConf().setAppName("MetaCacheSpark - Build");

			sparkConf.set("spark.sql.parquet.mergeSchema", "false");
			sparkConf.set("spark.shuffle.reduceLocality.enabled","false");
			//sparkConf.set("spark.memory.useLegacyMode","true");

			// Kryo serializer
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

			Class[] serializedClasses = {Feature.class, Sketch.class};
			sparkConf.registerKryoClasses(serializedClasses);


			//The ctx is created from the previous config
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			ctx.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

			LOG.warn("Using old Spark version!! - " + ctx.version());

			String buildArgs[] = newOptions.getOtherOptions();
			Build buildObject = new Build(buildArgs, ctx);

			buildObject.buildDatabase();
			LOG.warn("End of program ...");

			///ctx.close();
		}
		else if(newOptions.getMode() == MetaCacheOptions.Mode.QUERY) {
			SparkConf sparkConf = new SparkConf().setAppName("MetaCacheSpark - Query");

			sparkConf.set("spark.sql.parquet.mergeSchema", "false");
			sparkConf.set("spark.shuffle.reduceLocality.enabled","false");
			//sparkConf.set("spark.memory.useLegacyMode","true");

			// Kryo serializer
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

			Class[] serializedClasses = {Feature.class, Sketch.class};
			sparkConf.registerKryoClasses(serializedClasses);


			//The ctx is created from the previous config
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			ctx.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

			LOG.warn("Using old Spark version!! - " + ctx.version());

			// Get arguments and do my stuff
			String queryArgs[] = newOptions.getOtherOptions();
			Query queryObject = new Query(queryArgs, ctx);

			LOG.warn("End of program ...");
		}
		else {
			System.out.println("Not recognized option");
			newOptions.printHelp();
			//System.exit(1);
		}


		//System.exit(0);
	}
}
