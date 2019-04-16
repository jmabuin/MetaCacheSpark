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

import com.github.jmabuin.metacachespark.*;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by chema on 1/16/17.
 */
//public class Sketcher2Pair implements FlatMapFunction<Iterator<Sequence>,HashMap<Integer, ArrayList<LocationBasic>>> {
//public class Sketcher2PairPartitions implements FlatMapFunction<Iterator<Sequence>, HashMultiMapNative> {
public class Sketcher2PairPartitions2 implements PairFlatMapFunction<Iterator<Sequence>, Integer, LocationBasic> {
    private static final Log LOG = LogFactory.getLog(Sketcher2PairPartitions.class);

    private TreeMap<String, Long> sequencesIndexes;
    private int firstEmptyPosition;


    public Sketcher2PairPartitions2(TreeMap<String, Long> sequencesIndexes) {

        this.sequencesIndexes = sequencesIndexes;
        //this.firstEmptyPosition = 0;
        //this.lookup = new HashMap<Integer, Integer>();
    }

    @Override
    public Iterator<Tuple2<Integer, LocationBasic>> call(Iterator<Sequence> inputSequences){


        ArrayList<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();
        //HashMultiMapNative map = new HashMultiMapNative(254);
        HashMap<Integer, TreeSet<LocationBasic>> java_map = new HashMap<>();
        int currentStart;
        int currentEnd;

        //String currentWindow;
        int numWindows;
        int current_sketch_size;


        Comparator<LocationBasic> comparator = new Comparator<LocationBasic>() {
            public int compare(LocationBasic o1,
                               LocationBasic o2)
            {

                if (o1.getTargetId() > o2.getTargetId()) {
                    return 1;
                }

                if (o1.getTargetId() < o2.getTargetId()) {
                    return -1;
                }

                if (o1.getTargetId() == o2.getTargetId()) {
                    if (o1.getWindowId() > o2.getWindowId()) {
                        return 1;
                    }

                    if (o1.getWindowId() < o2.getWindowId()) {
                        return -1;
                    }

                    return 0;

                }
                return 0;

            }
        };

        //ArrayList<Tuple2<Integer, LocationBasic>> returnedValues_local = new ArrayList<Tuple2<Integer, LocationBasic>>();

        while(inputSequences.hasNext()) {
            Sequence inputSequence = inputSequences.next();


            currentStart = 0;
            currentEnd = MCSConfiguration.windowSize;

            //String currentWindow = "";
            //StringBuffer currentWindow = new StringBuffer();
            numWindows = 0;

            //LOG.warn("Processing sequence: " + inputSequence.getHeader());
            // We iterate over windows (with overlap)

            while (currentStart < (inputSequence.getData().length() - MCSConfiguration.kmerSize)) {
                //Sketch resultSketch = new Sketch();

                if (currentEnd > inputSequence.getData().length()) {
                    currentEnd = inputSequence.getData().length();
                }

                current_sketch_size = MCSConfiguration.sketchSize;

                if ((currentEnd - currentStart) >= MCSConfiguration.kmerSize) {

                    if (currentEnd - currentStart < MCSConfiguration.kmerSize * 2){
                        current_sketch_size = currentEnd - currentStart - MCSConfiguration.kmerSize + 1;
                    }

                    // We compute the k-mers. In C
                    int sketchValues[] = HashFunctions.window2sketch32(inputSequence.getData().substring(currentStart, currentEnd)
                            , current_sketch_size, MCSConfiguration.kmerSize);

                    if (sketchValues != null) {
                        //LOG.warn("[JMAbuin] CurrentWindow sketch size: " + sketchValues.length);

                        for (int newValue : sketchValues) {
							/*
							returnedValues_local.add(new Tuple2<Integer, LocationBasic>(newValue,
									new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows)));
							*/
                            //map.add(newValue, this.sequencesIndexes.get(inputSequence.getSeqId()).intValue(), numWindows);
                            if (!java_map.containsKey(newValue)) {
                                java_map.put(newValue, new TreeSet<LocationBasic>(comparator));
                            }

                            LocationBasic new_location = new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()).intValue(), numWindows);

                            if (java_map.get(newValue).size() < 254) {
                                java_map.get(newValue).add(new_location);
                            }
                            else {
                                if (java_map.get(newValue).ceiling(new_location) != null) {
                                    java_map.get(newValue).add(new_location);
                                    java_map.get(newValue).remove(java_map.get(newValue).last());
                                }
                            }

                        }
                    }


                }
                numWindows++;
                currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
                currentEnd = currentStart + MCSConfiguration.windowSize;

            }


        }
/*
        int total_deleted = map.post_process(false, false);
        //LOG.warn("Number of items in this partial map is: " + map.size());
        //LOG.warn("Number of deleted features: " + total_deleted);

        for (int key : map.keys()) {

            for(LocationBasic loc: map.get_locations(key)) {
                returnedValues.add(new Tuple2<Integer, LocationBasic>(key, loc));
            }

            map.clear_key(key);

        }

        map.clear();
*/
/*
        for(int key: java_map.keySet()) {

            List<LocationBasic> current_list = java_map.get(key);

            current_list.sort(new Comparator<LocationBasic>() {
                public int compare(LocationBasic o1,
                                   LocationBasic o2)
                {

                    if (o1.getTargetId() > o2.getTargetId()) {
                        return 1;
                    }

                    if (o1.getTargetId() < o2.getTargetId()) {
                        return -1;
                    }

                    if (o1.getTargetId() == o2.getTargetId()) {
                        if (o1.getWindowId() > o2.getWindowId()) {
                            return 1;
                        }

                        if (o1.getWindowId() < o2.getWindowId()) {
                            return -1;
                        }

                        return 0;

                    }
                    return 0;

                }
            });


            int i;

            for(i = 0; i< current_list.size() && i < 254; ++i) {
                returnedValues.add(new Tuple2<Integer, LocationBasic>(key, current_list.get(i)));
            }


        }

        java_map.clear();

        java_map = null;
*/

        for (int key : java_map.keySet()) {

            for(LocationBasic loc: java_map.get(key)) {
                returnedValues.add(new Tuple2<Integer, LocationBasic>(key, loc));
            }

            java_map.get(key).clear();

        }

        java_map.clear();


        return returnedValues.iterator();
    }


}
