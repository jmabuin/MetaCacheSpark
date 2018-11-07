package com.github.jmabuin.metacachespark;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.github.jmabuin.metacachespark.database.LocationBasicComparator;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * Created by jabuinmo on 30.01.17.
 */
public class MetaCacheSpark implements Serializable {

	private static final Log LOG = LogFactory.getLog(MetaCacheSpark.class);

	public static void main(String[] args) {

		long initTime = System.nanoTime();

		MetaCacheOptions newOptions = new MetaCacheOptions(args);

		if(newOptions.getMode() == EnumModes.Mode.HELP) {
			newOptions.printHelp();
		}
		else if(newOptions.getMode() == EnumModes.Mode.BUILD) {
			// Build mode entry point

			SparkConf sparkConf = new SparkConf().setAppName("MetaCacheSpark - Build");

			//sparkConf.set("spark.sql.parquet.mergeSchema", "false");
			sparkConf.set("spark.shuffle.reduceLocality.enabled","false");
			//sparkConf.set("spark.memory.useLegacyMode","true");
			//sparkConf.set("spark.storage.memoryFraction", "0.2");

			sparkConf.set("spark.sql.tungsten.enabled", "true");
			sparkConf.set("spark.io.compression.codec", "snappy");
			sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

			// Kryo serializer
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

			Class[] serializedClasses = {Location.class, Sketch.class, TreeMap.class};
			sparkConf.registerKryoClasses(serializedClasses);


			//The ctx is created from the previous config
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			//ctx.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

			LOG.info("Using Spark :: " + ctx.version());

			Build buildObject = new Build(newOptions, ctx);

			buildObject.buildDatabase();

            long endTime = System.nanoTime();

            LOG.info("End of program. Total time: " + ((endTime - initTime) / 1e9) + " seconds");

			///ctx.close();
		}
		else if(newOptions.getMode() == EnumModes.Mode.QUERY) {
			SparkConf sparkConf = new SparkConf().setAppName("MetaCacheSpark - Query");

			//sparkConf.set("spark.sql.parquet.mergeSchema", "false");
			sparkConf.set("spark.shuffle.reduceLocality.enabled","false");
			//sparkConf.set("spark.memory.useLegacyMode","true");
			//sparkConf.set("spark.storage.memoryFraction", "0.3");

			// Kryo serializer
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			sparkConf.set("spark.kryoserializer.buffer.max","512m");
			sparkConf.set("spark.driver.maxResultSize", "2g");

			sparkConf.set("spark.sql.tungsten.enabled", "true");
			sparkConf.set("spark.io.compression.codec", "snappy");
			sparkConf.set("spark.sql.parquet.compression.codec", "snappy");



			/*
			sparkConf.set("spark.sql.tungsten.enabled", "true")
sparkConf.set("spark.eventLog.enabled", "true")
sparkConf.set("spark.app.id", "YourApp")
sparkConf.set("spark.io.compression.codec", "snappy")
sparkConf.set("spark.rdd.compress", "true")
sparkConf.set("spark.streaming.backpressure.enabled", "true")

sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
			 */

/*
			TreeMap<LocationBasic, Integer> proba = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());

			Class[] serializedClasses = {Location.class,
					Sketch.class,
					LocationBasic.class,
					TreeMap.class,
					proba.getClass(),
					LocationBasicComparator.class,
					Integer.class};

			sparkConf.registerKryoClasses(serializedClasses);

			MapSerializer serializer = new MapSerializer();
*/
			sparkConf.set("spark.kryo.registrator","com.github.jmabuin.metacachespark.MyKryoRegistrator");


			//The ctx is created from the previous config
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			//ctx.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");
            ctx.setLogLevel("WARN");
			LOG.warn("Using old Spark version!! - " + ctx.version());

			// Get arguments and do my stuff
			//String queryArgs[] = newOptions.getOtherOptions();
			Query queryObject = new Query(newOptions, ctx);


            long endTime = System.nanoTime();

            LOG.info("End of program. Total time: " + ((endTime - initTime) / 1e9) + " seconds");
		}
		else {
			System.out.println("Not recognized option");
			newOptions.printHelp();
			//System.exit(1);
		}


		//System.exit(0);
	}
}
