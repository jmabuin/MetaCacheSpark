package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function2;

import java.util.HashMap;
import java.util.List;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducer implements Function2<HashMap<LocationBasic, Integer>, HashMap<LocationBasic, Integer>, HashMap<LocationBasic, Integer>> {

	@Override
	public HashMap<LocationBasic, Integer>call(HashMap<LocationBasic, Integer> v1, HashMap<LocationBasic, Integer> v2) {

		for(LocationBasic current_key : v2.keySet()) {

		    if(v1.containsKey(current_key)) {

		        v1.put(current_key, v1.get(current_key) + v2.get(current_key));

            }
            else {
                //if((v1.size() < 512) || (v2.get(current_key) > 1)) {
                    v1.put(current_key, v2.get(current_key));
                //}
            }


        }

        return v1;

	}

}
