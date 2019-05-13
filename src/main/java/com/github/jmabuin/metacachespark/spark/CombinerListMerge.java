package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

public class CombinerListMerge implements Function2<List<LocationBasic>, List<LocationBasic>, List<LocationBasic>> {


    @Override
    public List<LocationBasic> call(List<LocationBasic> locationBasics, List<LocationBasic> locationBasics2) throws Exception {
        locationBasics.addAll(locationBasics2);

        return locationBasics;
    }
}
