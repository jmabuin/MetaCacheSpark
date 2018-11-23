package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducerTreeMapNative implements Function2<TreeMap<LocationBasic, Integer>, TreeMap<LocationBasic, Integer>, TreeMap<LocationBasic, Integer>> {

    @Override
    public TreeMap<LocationBasic, Integer>call(TreeMap<LocationBasic, Integer> v1, TreeMap<LocationBasic, Integer> v2) {

        for(LocationBasic current_key : v2.keySet()) {

            if(v1.containsKey(current_key)) {

                v1.put(current_key, v1.get(current_key) + v2.get(current_key));

            }
            else {
                if((v1.size() < 256) || (v2.get(current_key) > 1)) {
                v1.put(current_key, v2.get(current_key));
                }
            }


        }

        return v1;

    }

}
