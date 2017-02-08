package com.github.metacachespark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by jabuinmo on 07.02.17.
 */
public class Query implements Serializable {

    private static final Log LOG = LogFactory.getLog(Query.class);

    // Default options values
    private QueryOptions param;

    private Database db;
    private JavaSparkContext jsc;

    public Query(String[] args, JavaSparkContext jsc) {

        this.param = new QueryOptions(args);

        this.jsc = jsc;

        this.db = new Database(jsc, this.param.getDbfile(), this.param);


    }

}
