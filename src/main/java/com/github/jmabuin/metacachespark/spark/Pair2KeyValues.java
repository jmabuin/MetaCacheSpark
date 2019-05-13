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

import com.github.jmabuin.metacachespark.IOSerialize;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.LocationKeyValues;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by chema on 4/21/17.
 */
public class Pair2KeyValues implements PairFlatMapFunction<Iterator<Tuple2<Integer, LocationBasic>>, Integer, LocationBasic> {
    private static final Log LOG = LogFactory.getLog(Pair2Locations.class);

    private MetaCacheOptions options;

    public Pair2KeyValues(MetaCacheOptions options) {
        this.options = options;
    }

    @Override
    public Iterator<Tuple2<Integer, LocationBasic>> call(Iterator<Tuple2<Integer,LocationBasic>> tuple2Iterator) throws Exception {

        List<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();

        HashMap<Integer, List<LocationBasic>> map = new HashMap<>();


        while(tuple2Iterator.hasNext()) {
            Tuple2<Integer, LocationBasic> currentItem = tuple2Iterator.next();

            if (!map.containsKey(currentItem._1())) {
                map.put(currentItem._1(), new ArrayList<LocationBasic>());
            }

            map.get(currentItem._1()).add(currentItem._2());
/*
            while (map.get(currentItem._1()).size() > 254) {


                map.get(currentItem._1()).remove(map.get(currentItem._1()).last());

            }
*/

        }

        tuple2Iterator = null;

        // If post process
        if (this.options.isRemove_overpopulated_features()) {
            long deleted_items = 0L;
            for(Integer key: map.keySet()) {

                if(map.get(key).size() >= 254) {
                    map.get(key).clear();
                    ++deleted_items;
                }

            }

            LOG.warn("Deleted items in first phase: " + deleted_items);
        }



        for(Map.Entry<Integer, List<LocationBasic>> item : map.entrySet()) {

            int key = item.getKey();

            for (LocationBasic location: item.getValue()) {
                returnedValues.add(new Tuple2<Integer, LocationBasic>(location.getTargetId(), new LocationBasic(key, location.getWindowId())));
            }


            map.get(key).clear();
        }

        map.clear();
        map = null;


        return returnedValues.iterator();
    }
}