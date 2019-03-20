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

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by chema on 4/4/17.
 */
public class Pair2HashMapNative implements Function2<Integer, Iterator<Tuple2<Integer, LocationBasic>>, Iterator<HashMultiMapNative>> {

    private static final Log LOG = LogFactory.getLog(Pair2HashMapNative.class);

    @Override
    public Iterator<HashMultiMapNative> call(Integer partitionId, Iterator<Tuple2<Integer, LocationBasic>> tuple2Iterator) throws Exception {

        LOG.warn("Starting to process partition: "+partitionId);

        int initialSize = 5;

        ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();

        HashMultiMapNative map = new HashMultiMapNative(254);

        while(tuple2Iterator.hasNext()) {
            Tuple2<Integer, LocationBasic> currentItem = tuple2Iterator.next();

            map.add(currentItem._1(), currentItem._2().getTargetId(), currentItem._2().getWindowId());

        }

        int total_deleted = map.post_process(false, false);

        LOG.warn("Number of deleted features: " + total_deleted);

        returnedValues.add(map);

        return returnedValues.iterator();
    }
}
