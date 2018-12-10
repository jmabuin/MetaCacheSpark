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
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class FilterPairRDD implements Function<Tuple2<Integer, List<LocationBasic>>, Boolean>{

    private Integer key;

    public FilterPairRDD(Integer key) {
        this.key = key;
    }

    @Override
    public Boolean call(Tuple2<Integer, List<LocationBasic>> integerListTuple2) throws Exception{
        if(this.key == integerListTuple2._1) {

            return true;
        }
        else {
            return false;
        }
    }
}
