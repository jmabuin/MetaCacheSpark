package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Locations;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class Pair2Locations implements Function<Tuple2<Integer, List<LocationBasic>>, Locations> {
    @Override
    public Locations call(Tuple2<Integer, List<LocationBasic>> integerListTuple2) throws Exception {

        Locations newLocs = new Locations();

        newLocs.setKey(integerListTuple2._1);
        newLocs.setLocations(integerListTuple2._2);

        return newLocs;

    }
}