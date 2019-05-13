package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SketchesMap2Boolean implements FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer> {
    private static final Log LOG = LogFactory.getLog(SketchesMap2Boolean.class);
    private MetaCacheOptions options;

    public SketchesMap2Boolean(MetaCacheOptions options) {
        this.options = options;

    }

    @Override
    public Iterator<Integer> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
        List<Integer> returned_values = new ArrayList<>();

        Long num_items = 0L;

        while(tuple2Iterator.hasNext()) {
            Tuple2<Integer, Integer> current = tuple2Iterator.next();

            if (current._2() >= options.getProperties().getMax_locations_per_feature()){
                returned_values.add(current._1());
                num_items++;
            }



        }
        LOG.warn("Number of deleted items in this executor is: " + num_items);
        return returned_values.iterator();

    }
}
