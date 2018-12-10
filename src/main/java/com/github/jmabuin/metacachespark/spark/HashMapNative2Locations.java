package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HashMapNative2Locations implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Integer, Location> {


    @Override
    public Iterator<Tuple2<Integer, Location>> call(Iterator<HashMultiMapNative> hm_iterator) {

        List<Tuple2<Integer, Location>> return_values = new ArrayList<>();

        while(hm_iterator.hasNext()) {

            HashMultiMapNative hm = hm_iterator.next();

            int[] keyset = hm.keys();

            for(int key: keyset) {

                int[] values = hm.get(key);

                for(int i = 0; i< values.length; i+=2){
                    return_values.add(new Tuple2<Integer, Location>(values[i], new Location(key, values[i], values[i+1])));
                }

            }

        }

        return return_values.iterator();
    }

}
