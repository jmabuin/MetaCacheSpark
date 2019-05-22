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
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Locations2HashMap implements FlatMapFunction<Iterator<Tuple2<Integer, LocationBasic>>, HashMap<Integer, List<LocationBasic>>> {

    private static final Log LOG = LogFactory.getLog(Locations2HashMap.class);

    @Override
    public Iterator<HashMap<Integer, List<LocationBasic>>> call(Iterator<Tuple2<Integer, LocationBasic>> inputLocations){

        ArrayList<HashMap<Integer, List<LocationBasic>>> returnedValues = new ArrayList<HashMap<Integer, List<LocationBasic>>>();

        HashMap<Integer, List<LocationBasic>> map = new HashMap<Integer, List<LocationBasic>>();

        while(inputLocations.hasNext()) {
            Tuple2<Integer, LocationBasic> current = inputLocations.next();
            int tgtid = current._1();

            if (!map.containsKey(current._2().getTargetId())) {
                map.put(current._2().getTargetId(), new ArrayList<LocationBasic>());
            }

            map.get(current._2().getTargetId()).add(new LocationBasic(tgtid, current._2().getWindowId()));



        }

        //int total_deleted = map.post_process(true, false);

        //LOG.warn("Number of deleted features: " + total_deleted);

        returnedValues.add(map);

        return returnedValues.iterator();


    }

}