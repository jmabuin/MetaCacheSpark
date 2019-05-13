package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function;

public class CombinerCreate implements Function<LocationBasic, Integer> {
    @Override
    public Integer call(LocationBasic locationBasic) throws Exception {

        return 1;

    }
}
