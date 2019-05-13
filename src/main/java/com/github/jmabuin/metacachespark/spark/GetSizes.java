package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GetSizes implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Integer, Integer> {
    @Override
    public Iterator<Tuple2<Integer, Integer>> call(Iterator<HashMultiMapNative> hashMultiMapNativeIterator) throws Exception {
        List<Tuple2<Integer, Integer>> return_values = new ArrayList<>();

        while(hashMultiMapNativeIterator.hasNext()) {

            HashMultiMapNative map = hashMultiMapNativeIterator.next();

            int[] keys = map.keys();

            for(int key: keys) {
                return_values.add(new Tuple2<Integer, Integer>(key, map.get_size_of_key(key)));
            }



        }

        return return_values.iterator();

    }
}
