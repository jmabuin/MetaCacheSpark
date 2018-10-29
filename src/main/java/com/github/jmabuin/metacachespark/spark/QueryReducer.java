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

		/*if(v1.size() + v2.size() < 256) {
			v1.addAll(v2);
		}
		else {
			if (v1.size() <= 256) {

				int currentSize;
				int i;
				for(currentSize = v1.size(), i = 0; currentSize < 256 && i< v2.size(); currentSize++, i++) {
					v1.add(v2.get(i));
				}
			}

		}
		return v1;
		*/
	}

}
