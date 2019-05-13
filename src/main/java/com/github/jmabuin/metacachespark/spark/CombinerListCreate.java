package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function;

import java.util.List;

public class CombinerListCreate implements Function<List<LocationBasic>, List<LocationBasic>> {


    @Override
    public List<LocationBasic> call(List<LocationBasic> locationBasics) throws Exception {
        return locationBasics;
    }
}
