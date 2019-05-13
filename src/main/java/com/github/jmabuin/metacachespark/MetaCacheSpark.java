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
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.database.LocationBasicComparator;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

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
            //sparkConf.set("spark.shuffle.reduceLocality.enabled","false");
            sparkConf.set("spark.memory.fraction", "0.14");
            //sparkConf.set("spark.memory.storageFraction", "0.7");
            //sparkConf.set("spark.memory.useLegacyMode","true");
            //sparkConf.set("spark.storage.memoryFraction", "0.2");

            sparkConf.set("spark.executor.extraJavaOptions","-XX:+UseG1GC");

            sparkConf.set("spark.sql.tungsten.enabled", "true");
            sparkConf.set("spark.io.compression.codec", "snappy");
            sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

            sparkConf.set("spark.driver.maxResultSize", "6g");

            // Kryo serializer
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            sparkConf.set("spark.kryoserializer.buffer.max","2047m");

            Class[] serializedClasses = {Location.class,
                    Sketch.class,
                    TreeMap.class,
                    TreeSet.class,
                    HashMap.class,
                    LocationBasic.class,
                    MatchCandidate.class,
                    List.class,
                    Integer.class,
                    String.class,
                    Long.class,
                    Sequence.class,
                    HashMultiMapNative.class
            };
            sparkConf.registerKryoClasses(serializedClasses);


            //The ctx is created from the previous config
            JavaSparkContext ctx = new JavaSparkContext(sparkConf);
            ctx.setLogLevel("WARN");
            //ctx.setCheckpointDir("checkpointing/");
            //ctx.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

            LOG.warn("Using Spark :: " + ctx.version());

            Build buildObject = new Build(newOptions, ctx);

            buildObject.buildDatabase();

            long endTime = System.nanoTime();

            LOG.warn("[BUILD] End of program with " + newOptions.getPartitions() + " DB: " + newOptions.getDbfile() + ". Total time: " + ((endTime - initTime) / 1e9) + " seconds");

            ///ctx.close();
        }
        else if(newOptions.getMode() == EnumModes.Mode.QUERY) {
            SparkConf sparkConf = new SparkConf().setAppName("MetaCacheSpark - Query");

            //sparkConf.set("spark.sql.parquet.mergeSchema", "false");
            //sparkConf.set("spark.shuffle.reduceLocality.enabled","false");
            //sparkConf.set("spark.memory.useLegacyMode","true");
            //sparkConf.set("spark.storage.memoryFraction", "0.3");

            // Kryo serializer
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            sparkConf.set("spark.kryoserializer.buffer.max","1024m");
            sparkConf.set("spark.driver.maxResultSize", "4g");

            sparkConf.set("spark.sql.tungsten.enabled", "true");
            sparkConf.set("spark.io.compression.codec", "snappy");
            sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

            if (newOptions.getNumThreads() != 1) {
                sparkConf.set("spark.executor.cores", String.valueOf(newOptions.getNumThreads()));
            }

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
            //sparkConf.set("spark.kryo.registrator","com.github.jmabuin.metacachespark.MyKryoRegistrator");
            Class[] serializedClasses = {Location.class, Sketch.class, TreeMap.class, LocationBasic.class, MatchCandidate.class, List.class, HashMultiMapNative.class};
            sparkConf.registerKryoClasses(serializedClasses);


            //The ctx is created from the previous config
            JavaSparkContext ctx = new JavaSparkContext(sparkConf);
            //ctx.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");
            ctx.setLogLevel("WARN");
            LOG.warn("Using Spark version - " + ctx.version());

            // Get arguments and do my stuff
            //String queryArgs[] = newOptions.getOtherOptions();
            Query queryObject = new Query(newOptions, ctx);


            long endTime = System.nanoTime();

            LOG.warn("[QUERY] End of program for " + newOptions.getOutfile() + ". Total time: " + ((endTime - initTime) / 1e9) + " seconds");
        }
        else {
            System.out.println("Not recognized option");
            newOptions.printHelp();
            //System.exit(1);
        }


        //System.exit(0);
    }
}
