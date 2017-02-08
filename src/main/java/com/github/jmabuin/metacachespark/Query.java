package com.github.jmabuin.metacachespark;
import com.github.jmabuin.metacachespark.options.QueryOptions;
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

    public Query(QueryOptions param, JavaSparkContext jsc) {

        this.param = param;

        this.jsc = jsc;

        this.db = new Database(jsc, this.param.getDbfile(), this.param);


    }

}
