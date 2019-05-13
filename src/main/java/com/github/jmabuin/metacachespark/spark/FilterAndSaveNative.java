package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FilterAndSaveNative implements FlatMapFunction<Iterator<HashMultiMapNative>, HashMultiMapNative> {

    private List<Integer> items_to_delete;

    public FilterAndSaveNative(List<Integer> items_to_delete) {
        this.items_to_delete = items_to_delete;
    }

    @Override
    public Iterator<HashMultiMapNative> call(Iterator<HashMultiMapNative> hashMultiMapsNative) throws Exception {

        System.gc();
        List<HashMultiMapNative> return_maps = new ArrayList<>();


        while(hashMultiMapsNative.hasNext()) {

            HashMultiMapNative hashMultiMapNative = hashMultiMapsNative.next();

            for(Integer item: items_to_delete) {
                hashMultiMapNative.clear_key(item.intValue());

            }


            return_maps.add(hashMultiMapNative);

        }




        return return_maps.iterator();


    }
}
