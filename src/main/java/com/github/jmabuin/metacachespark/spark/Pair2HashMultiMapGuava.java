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
import com.google.common.collect.HashMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Pair2HashMultiMapGuava implements Function2<Integer, Iterator<Location>, Iterator<HashMultimap<Integer, LocationBasic>>> {

    private static final Log LOG = LogFactory.getLog(Pair2HashMultiMapGuava.class);

    @Override
    public Iterator<HashMultimap<Integer, LocationBasic>> call(Integer partitionId, Iterator<Location> tuple2Iterator) throws Exception {

        LOG.warn("Starting to process partition: "+partitionId);

        int initialSize = 5;

        ArrayList<HashMultimap<Integer, LocationBasic>> returnedValues = new ArrayList<HashMultimap<Integer, LocationBasic>>();

        HashMultimap<Integer, LocationBasic> map = HashMultimap.create();

        while(tuple2Iterator.hasNext()) {
            Location currentItem = tuple2Iterator.next();

            Integer key = currentItem.getKey();
            LocationBasic current_location = new LocationBasic(currentItem.getTargetId(), currentItem.getWindowId());


            map.put(key, current_location);


        }

        returnedValues.add(map);

        return returnedValues.iterator();
    }
}
