package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Pair2HashMap implements Function2<Integer, Iterator<Tuple2<Integer, LocationBasic>>, Iterator<HashMap<Integer, List<LocationBasic>>>> {

    private static final Log LOG = LogFactory.getLog(Pair2HashMap.class);

    @Override
    public Iterator<HashMap<Integer, List<LocationBasic>>> call(Integer partitionId, Iterator<Tuple2<Integer, LocationBasic>> tuple2Iterator) throws Exception {

        LOG.warn("Starting to process partition: "+partitionId);

        int initialSize = 5;

        ArrayList<HashMap<Integer, List<LocationBasic>>> returnedValues = new ArrayList<HashMap<Integer, List<LocationBasic>>>();

        HashMap<Integer, List<LocationBasic>> map = new HashMap<Integer, List<LocationBasic>>();

        while(tuple2Iterator.hasNext()) {
            Tuple2<Integer, LocationBasic> currentItem = tuple2Iterator.next();

            Integer key = currentItem._1;
            LocationBasic current_location = new LocationBasic(currentItem._2().getTargetId(), currentItem._2().getWindowId());

            if (map.containsKey(key) && map.get(key).size() < 32) {
                map.get(key).add(current_location);
            }
            else if (!map.containsKey(key)){
                List<LocationBasic> new_list = new ArrayList<LocationBasic>();
                new_list.add(current_location);
                map.put(key, new_list);
            }
            //map.add(currentItem._1(), currentItem._2().getTargetId(), currentItem._2().getWindowId());

        }

        returnedValues.add(map);

        return returnedValues.iterator();
    }
}
