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
