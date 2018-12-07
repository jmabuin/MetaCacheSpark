package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Locations2HashMapNative implements FlatMapFunction<Iterator<List<Location>>, HashMultiMapNative> {

    private static final Log LOG = LogFactory.getLog(Locations2HashMapNative.class);

    @Override
    public Iterator<HashMultiMapNative> call(Iterator<List<Location>> inputLocations){

        ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();

        HashMultiMapNative map = new HashMultiMapNative();

        while(inputLocations.hasNext()) {
            List<Location> currentList = inputLocations.next();

            for (Location current_location: currentList) {
                map.add(current_location.getKey(), current_location.getTargetId(), current_location.getWindowId());
            }



        }

        //int total_deleted = map.post_process(true, false);

        //LOG.warn("Number of deleted features: " + total_deleted);

        returnedValues.add(map);

        return returnedValues.iterator();


    }

}
