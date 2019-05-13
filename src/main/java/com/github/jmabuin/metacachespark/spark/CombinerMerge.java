package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.List;
import java.util.TreeSet;

public class CombinerMerge implements Function2<Integer, LocationBasic, Integer> {

    @Override
    public Integer call(Integer locationBasics, LocationBasic locationBasics2) throws Exception {

        return locationBasics + 1;


    }

}
