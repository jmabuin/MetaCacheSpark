package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SketchesMap implements PairFunction<Tuple2<Integer, LocationBasic>, Integer, Integer > {
    @Override
    public Tuple2<Integer, Integer> call(Tuple2<Integer, LocationBasic> integerLocationBasicTuple2) throws Exception {
        return new Tuple2<Integer, Integer>(integerLocationBasicTuple2._1(), 1);
    }
}
