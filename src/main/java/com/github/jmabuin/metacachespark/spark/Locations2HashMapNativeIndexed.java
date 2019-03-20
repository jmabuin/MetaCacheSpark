package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Locations2HashMapNativeIndexed implements Function2<Integer, Iterator<Location>, Iterator<HashMultiMapNative>> {

    private static final Log LOG = LogFactory.getLog(Locations2HashMapNativeIndexed.class);

    @Override
    public Iterator<HashMultiMapNative> call(Integer partitionId, Iterator<Location> tuple2Iterator) throws Exception {

        LOG.warn("Starting to process partition: " + partitionId);

        int initialSize = 5;

        ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();

        HashMultiMapNative map = new HashMultiMapNative(254);

        /*tuple2Iterator.forEachRemaining(new Consumer<Location>() {
            @Override
            public void accept(Location location) {
                map.add(location.getKey(), location.getTargetId(), location.getWindowId());
            }
        });
        */
        while (tuple2Iterator.hasNext()) {
            Location currentItem = tuple2Iterator.next();

            map.add(currentItem.getKey(), currentItem.getTargetId(), currentItem.getWindowId());

            tuple2Iterator.remove();

        }

        int total_deleted = map.post_process(false, false);

        LOG.warn("Number of deleted features: " + total_deleted);

        returnedValues.add(map);

        return returnedValues.iterator();
    }
}