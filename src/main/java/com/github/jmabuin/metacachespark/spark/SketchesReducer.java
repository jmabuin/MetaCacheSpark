package com.github.jmabuin.metacachespark.spark;

import org.apache.spark.api.java.function.Function2;

public class SketchesReducer implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer+integer2;
    }
}
