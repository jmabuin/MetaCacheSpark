package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

public class FilterAndSave implements FlatMapFunction<Iterator<Tuple2<Integer, LocationBasic>>, HashMultiMapNative> {

    private TreeSet<Integer> delete_features;

    public FilterAndSave(TreeSet<Integer> delete_features) {
        this.delete_features = delete_features;

    }

    @Override
    public Iterator<HashMultiMapNative> call(Iterator<Tuple2<Integer, LocationBasic>> tuple2Iterator) throws Exception {


        System.gc();

        ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();
        HashMultiMapNative map = new HashMultiMapNative(254);

        while(tuple2Iterator.hasNext()){
            Tuple2<Integer, LocationBasic> new_item = tuple2Iterator.next();

            if (!this.delete_features.contains(new_item._1())) {
                map.add(new_item._1(), new_item._2().getTargetId(), new_item._2().getWindowId());
            }




        }


        returnedValues.add(map);


        return returnedValues.iterator();
    }
}
