package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

public class CombinerMergeCombiners implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer locationBasics, Integer locationBasics2) throws Exception {

        return locationBasics + locationBasics2;
    }
}
