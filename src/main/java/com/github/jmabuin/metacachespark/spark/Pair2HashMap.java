/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
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

public class Pair2HashMap implements Function2<Integer, Iterator<Location>, Iterator<HashMap<Integer, List<LocationBasic>>>> {

    private static final Log LOG = LogFactory.getLog(Pair2HashMap.class);

    @Override
    public Iterator<HashMap<Integer, List<LocationBasic>>> call(Integer partitionId, Iterator<Location> tuple2Iterator) throws Exception {

        LOG.warn("Starting to process partition: "+partitionId);

        int initialSize = 5;

        ArrayList<HashMap<Integer, List<LocationBasic>>> returnedValues = new ArrayList<HashMap<Integer, List<LocationBasic>>>();

        HashMap<Integer, List<LocationBasic>> map = new HashMap<Integer, List<LocationBasic>>();

        while(tuple2Iterator.hasNext()) {
            Location currentItem = tuple2Iterator.next();

            Integer key = currentItem.getKey();
            LocationBasic current_location = new LocationBasic(currentItem.getTargetId(), currentItem.getWindowId());

            if (map.containsKey(key)) {
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
