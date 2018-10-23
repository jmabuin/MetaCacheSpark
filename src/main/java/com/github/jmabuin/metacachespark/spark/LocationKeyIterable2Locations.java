package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Locations;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class LocationKeyIterable2Locations implements Function<Tuple2<Integer, Iterable<LocationBasic>>, Locations> {

    private int max_locations = 256;

    public LocationKeyIterable2Locations(int max_locations) {
        this.max_locations = max_locations;
    }

    public LocationKeyIterable2Locations() {

    }

    @Override
    public Locations call(Tuple2<Integer, Iterable<LocationBasic>> input_data) {

        List<LocationBasic> new_list = new ArrayList<LocationBasic>();

        for(LocationBasic new_location: input_data._2) {
            new_list.add(new_location);

            if (new_list.size() >= this.max_locations) {
                break;
            }
        }



        return new Locations(input_data._1, new_list);
    }
}
