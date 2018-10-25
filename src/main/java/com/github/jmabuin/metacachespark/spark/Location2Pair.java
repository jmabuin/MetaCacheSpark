package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Location2Pair implements PairFlatMapFunction<Iterator<Location>, Integer, LocationBasic> {

    @Override
    public Iterator<Tuple2<Integer, LocationBasic>> call(Iterator<Location> data) {

        List<Tuple2<Integer, LocationBasic>> return_data = new ArrayList<Tuple2<Integer, LocationBasic>>();

        while(data.hasNext()) {
            Location current_location = data.next();

            int key = current_location.getKey();
            int target_id = current_location.getTargetId();
            int window_id = current_location.getWindowId();

            LocationBasic new_location_basic = new LocationBasic(target_id, window_id);

            return_data.add(new Tuple2<Integer, LocationBasic>(key, new_location_basic));

        }

        return return_data.iterator();
    }
}
