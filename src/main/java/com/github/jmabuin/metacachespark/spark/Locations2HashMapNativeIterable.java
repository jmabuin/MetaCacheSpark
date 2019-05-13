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
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class Locations2HashMapNativeIterable implements FlatMapFunction<Iterator<Tuple2<Integer, List<LocationBasic>>>, HashMultiMapNative> {

    private static final Log LOG = LogFactory.getLog(Locations2HashMapNativeIterable.class);

    private MetaCacheOptions options;

    public Locations2HashMapNativeIterable(MetaCacheOptions options) {
        this.options = options;
    }

    @Override
    public Iterator<HashMultiMapNative> call(Iterator<Tuple2<Integer, List<LocationBasic>>> inputLocations){

        ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();

        HashMultiMapNative map = new HashMultiMapNative(254);

        while(inputLocations.hasNext()) {
            Tuple2<Integer, List<LocationBasic>> current = inputLocations.next();
            int tgtid = current._1();

            for (LocationBasic current_location: current._2) {
                map.add(current_location.getTargetId(), tgtid, current_location.getWindowId());
            }


        }

        int total_deleted = map.post_process(this.options.isRemove_overpopulated_features(), false);

        LOG.warn("Number of deleted features: " + total_deleted);

        returnedValues.add(map);

        return returnedValues.iterator();


    }

}
