package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.IOSerialize;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.LocationKeyValues;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chema on 4/21/17.
 */
public class Pair2KeyValues implements Function<Tuple2<Integer, ArrayList<LocationBasic>>, LocationKeyValues> {
    @Override
    public LocationKeyValues call(Tuple2<Integer, ArrayList<LocationBasic>> integerListTuple2) throws Exception {
        return new LocationKeyValues(integerListTuple2._1(), IOSerialize.toString(integerListTuple2._2()));
    }
}