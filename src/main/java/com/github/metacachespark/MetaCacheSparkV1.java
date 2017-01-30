package com.github.metacachespark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by jabuinmo on 30.01.17.
 */
public class MetaCacheSparkV1 implements Serializable {

    private static final Log LOG = LogFactory.getLog(MetaCacheSparkV1.class);

    public static void main(String[] args) {

        MetaCacheOptions newOptions = new MetaCacheOptions(args);

        if(newOptions.getMode() == MetaCacheOptions.Mode.HELP) {
            newOptions.printHelp();
        }
        else if(newOptions.getMode() == MetaCacheOptions.Mode.BUILD) {
            // Build mode entry point

            SparkConf sparkConf = new SparkConf().setAppName("MetaCacheSpark - Build");

            //The ctx is created from scratch
            JavaSparkContext ctx = new JavaSparkContext(sparkConf);

            LOG.warn("Using old Spark version!! - " + ctx.version());

            String buildArgs[] = newOptions.getOtherOptions();
            Build buildObject = new Build(buildArgs, ctx);

            buildObject.buildDatabase();
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
