package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducerTreeMapNative implements Function2<List<LocationBasic>, List<LocationBasic>, List<LocationBasic>> {

    @Override
    public List<LocationBasic>call(List<LocationBasic> v1, List<LocationBasic> v2) {

        v1.addAll(v2);

        return v1;

    }

}
