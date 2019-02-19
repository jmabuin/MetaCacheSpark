package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PostProcessing implements FlatMapFunction<Iterator<HashMultiMapNative>, HashMultiMapNative> {

    private static final Log LOG = LogFactory.getLog(PostProcessing.class);

    @Override
    public Iterator<HashMultiMapNative> call(Iterator<HashMultiMapNative> input_maps){

        int total_deleted = 0;

        while(input_maps.hasNext()) {

            total_deleted += input_maps.next().post_process(true, false);

        }
        LOG.warn("Number of deleted features: " + total_deleted);
        return input_maps;


    }

}
