package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.io.MapUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by chema on 3/30/17.
 */


public class QueryReducerTreeMap implements Function2<TreeMap<LocationBasic, Integer>, TreeMap<LocationBasic, Integer>, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(QueryReducerTreeMap.class);


    @Override
    public TreeMap<LocationBasic, Integer> call(TreeMap<LocationBasic, Integer> v1, TreeMap<LocationBasic, Integer> v2) {

        //LOG.warn("Starting reducer: "+v1.size()+" :: "+v2.size());

        for(Map.Entry<LocationBasic, Integer> currentEntry: v2.entrySet()) {

            if(v1.containsKey(currentEntry.getKey())) {
                v1.put(currentEntry.getKey(), currentEntry.getValue() + v1.get(currentEntry.getKey()));
            }
            else {
                //if(v1.size()<256 || currentEntry.getValue() >= 2) {
                    v1.put(currentEntry.getKey(), currentEntry.getValue());
                //}

            }

        }

        //LOG.warn("Final size "+v1.size());
        return v1;


/*
        HashMap<LocationBasic, Integer> newMap = new HashMap<LocationBasic, Integer>();
        TreeMap<LocationBasic, Integer> newTreeMap = new TreeMap<LocationBasic, Integer>();

        TreeMap<Integer, List<LocationBasic>> reverseTreeMap = new TreeMap<Integer, List<LocationBasic>>();

        for(Map.Entry<LocationBasic, Integer> currentEntry: v1.entrySet()) {
            if(newMap.containsKey(currentEntry.getKey())) {
                newMap.put(currentEntry.getKey(), newMap.get(currentEntry.getKey()) + currentEntry.getValue());
            }
            else {
                newMap.put(currentEntry.getKey(), currentEntry.getValue());
            }
        }

        for(Map.Entry<LocationBasic, Integer> currentEntry: v2.entrySet()) {
            if(newMap.containsKey(currentEntry.getKey())) {
                newMap.put(currentEntry.getKey(), newMap.get(currentEntry.getKey()) + currentEntry.getValue());
            }
            else {
                newMap.put(currentEntry.getKey(), currentEntry.getValue());
            }
        }



        int currentItemNumber = 0;
        int allowedItems = 64;

        for(Map.Entry<LocationBasic, Integer> currentEntry: newMap.entrySet()) {

            if(reverseTreeMap.containsKey(currentEntry.getValue())) {
                reverseTreeMap.get(currentEntry.getValue()).add(currentEntry.getKey());
            }
            else {
                List<LocationBasic> newList = new ArrayList<LocationBasic>();
                newList.add(currentEntry.getKey());
                reverseTreeMap.put(currentEntry.getValue(), newList);
            }
            //reverseTreeMap.put(currentEntry.getValue(), currentEntry.getKey());

        }

        for(Map.Entry<Integer, List<LocationBasic>> currentEntry: reverseTreeMap.descendingMap().entrySet()) {

            if(currentItemNumber < allowedItems) {
                for (LocationBasic currentLocation : currentEntry.getValue()) { //currentItemNumber < allowedItems) {
                    if(currentItemNumber < allowedItems) {
                        newTreeMap.put(currentLocation, currentEntry.getKey());
                        currentItemNumber++;
                    }
                }
            }

        }

        return newTreeMap;
*/
    }

}
