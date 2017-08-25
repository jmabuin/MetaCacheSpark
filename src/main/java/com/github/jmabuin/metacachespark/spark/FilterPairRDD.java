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
