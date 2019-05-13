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
import com.github.jmabuin.metacachespark.database.LocationBasicComparator;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by chema on 4/4/17.
 */
public class Pair2LocationsIterable implements PairFlatMapFunction<Iterator<Tuple2<Integer, List<LocationBasic>>>, Integer, List<LocationBasic>> {

    private static final Log LOG = LogFactory.getLog(Pair2LocationsIterable.class);

    private MetaCacheOptions options;

    public Pair2LocationsIterable(MetaCacheOptions options){
        this.options = options;
    }

    @Override
    public Iterator<Tuple2<Integer, List<LocationBasic>>> call(Iterator<Tuple2<Integer,List<LocationBasic>>> tuple2Iterator) throws Exception {

        List<Tuple2<Integer, List<LocationBasic>>> returnedValues = new ArrayList<Tuple2<Integer, List<LocationBasic>>>();

        HashMap<Integer, TreeSet<LocationBasic>> map = new HashMap<>();


        while(tuple2Iterator.hasNext()) {
            Tuple2<Integer, List<LocationBasic>> currentItem = tuple2Iterator.next();

            if (!map.containsKey(currentItem._1())) {
                map.put(currentItem._1(), new TreeSet<LocationBasic>(new LocationBasicComparator()));
            }

            map.get(currentItem._1()).addAll(currentItem._2());

            //for(LocationBasic current_location: currentItem._2()) {
            //    map.get(currentItem._1()).add(current_location);

                /*if (map.get(currentItem._1()).size() > 254) {


                    map.get(currentItem._1()).remove(map.get(currentItem._1()).last());

                }*/
            //}

        }

        tuple2Iterator = null;

        // If post process
        if(this.options.isRemove_overpopulated_features()) {
            for(Integer key: map.keySet()) {

                if(map.get(key).size() >= 254) {
                    map.get(key).clear();
                }

            }
        }



        for(Map.Entry<Integer, TreeSet<LocationBasic>> item : map.entrySet()) {

            int key = item.getKey();

            for (LocationBasic location: item.getValue()) {
                List<LocationBasic> new_list = new ArrayList<>();
                new_list.add(new LocationBasic(key, location.getWindowId()));
                returnedValues.add(new Tuple2<Integer, List<LocationBasic>>(location.getTargetId(), new_list));
            }


            map.get(key).clear();
        }

        map.clear();
        map = null;


        return returnedValues.iterator();


    }
}
