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
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class Locations2HashMultiMapGuava2 implements FlatMapFunction<Iterator<Tuple2<Integer, LocationBasic>>, HashMultimap<Integer, LocationBasic>> {

    private static final Log LOG = LogFactory.getLog(Locations2HashMultiMapGuava2.class);

    @Override
    public Iterator<HashMultimap<Integer, LocationBasic>> call(Iterator<Tuple2<Integer, LocationBasic>> inputLocations){

        ArrayList<HashMultimap<Integer, LocationBasic>> returnedValues = new ArrayList<HashMultimap<Integer, LocationBasic>>();

        HashMultimap<Integer, LocationBasic> map = HashMultimap.create();
        HashMap<Integer, List<LocationBasic>> tmp_map = new HashMap<Integer, List<LocationBasic>>();

        while(inputLocations.hasNext()) {
            Tuple2<Integer, LocationBasic> current = inputLocations.next();
            int key = current._1();

            if (!tmp_map.containsKey(current._2().getTargetId())) {
                tmp_map.put(key, new ArrayList<LocationBasic>());
            }

            tmp_map.get(key).add(new LocationBasic(current._2().getTargetId(), current._2().getWindowId()));

        }

        //Limit to 254 features per key

        for(int key:tmp_map.keySet()) {

            List<LocationBasic> current_list = tmp_map.get(key);

            current_list.sort(new Comparator<LocationBasic>() {
                public int compare(LocationBasic o1,
                                   LocationBasic o2)
                {

                    if (o1.getTargetId() > o2.getTargetId()) {
                        return 1;
                    }

                    if (o1.getTargetId() < o2.getTargetId()) {
                        return -1;
                    }

                    if (o1.getTargetId() == o2.getTargetId()) {
                        if (o1.getWindowId() > o2.getWindowId()) {
                            return 1;
                        }

                        if (o1.getWindowId() < o2.getWindowId()) {
                            return -1;
                        }

                        return 0;

                    }
                    return 0;

                }
            });

            int i;

            for(i = 0; i< current_list.size() && i < 254; ++i) {
                //new_list.add(current_list.get(i));
                map.put(key, current_list.get(i));
            }

            current_list.clear();


        }


        returnedValues.add(map);

        return returnedValues.iterator();


    }

}
