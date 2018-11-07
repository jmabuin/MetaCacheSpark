package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.google.common.collect.HashMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Pair2HashMultiMapGuava implements Function2<Integer, Iterator<Tuple2<Integer, LocationBasic>>, Iterator<HashMultimap<Integer, LocationBasic>>> {

    private static final Log LOG = LogFactory.getLog(Pair2HashMultiMapGuava.class);

    @Override
    public Iterator<HashMultimap<Integer, LocationBasic>> call(Integer partitionId, Iterator<Tuple2<Integer, LocationBasic>> tuple2Iterator) throws Exception {

        LOG.warn("Starting to process partition: "+partitionId);

        int initialSize = 5;

        ArrayList<HashMultimap<Integer, LocationBasic>> returnedValues = new ArrayList<HashMultimap<Integer, LocationBasic>>();

        HashMultimap<Integer, LocationBasic> map = HashMultimap.create();

        while(tuple2Iterator.hasNext()) {
            Tuple2<Integer, LocationBasic> currentItem = tuple2Iterator.next();

            Integer key = currentItem._1;
            LocationBasic current_location = new LocationBasic(currentItem._2().getTargetId(), currentItem._2().getWindowId());

            if (map.containsKey(key) && map.get(key).size() < 256) {
                map.get(key).add(current_location);
            }
            else if (!map.containsKey(key)){
                map.get(key).add(current_location);
            }
            //map.add(currentItem._1(), currentItem._2().getTargetId(), currentItem._2().getWindowId());

        }

        returnedValues.add(map);

        return returnedValues.iterator();
    }
}
