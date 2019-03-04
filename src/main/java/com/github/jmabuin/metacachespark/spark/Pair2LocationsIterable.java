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
public class Pair2LocationsIterable implements PairFlatMapFunction<Iterator<Tuple2<Integer, Iterable<LocationBasic>>>, Integer, LocationBasic> {

    private static final Log LOG = LogFactory.getLog(Pair2LocationsIterable.class);

    @Override
    public Iterator<Tuple2<Integer, LocationBasic>> call(Iterator<Tuple2<Integer,Iterable<LocationBasic>>> tuple2Iterator) throws Exception {

        List<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();


        while(tuple2Iterator.hasNext()) {

            Tuple2<Integer,Iterable<LocationBasic>> current_data = tuple2Iterator.next();

            int current_key = current_data._1();
            List<LocationBasic> current_list = Lists.newArrayList(current_data._2());

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
                returnedValues.add(new Tuple2<Integer, LocationBasic>(current_list.get(i).getTargetId(), new LocationBasic(current_key, current_list.get(i).getWindowId())));
            }



        }


        return returnedValues.iterator();


    }
}
