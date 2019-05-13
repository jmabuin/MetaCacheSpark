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
public class Sketcher2PairList implements PairFlatMapFunction<Sequence,Integer, List<LocationBasic>> {
    private static final Log LOG = LogFactory.getLog(Sketcher2Pair.class);

    private TreeMap<String, Long> sequencesIndexes;

    public Sketcher2PairList(TreeMap<String, Long> sequencesIndexes) {
        //super();
        this.sequencesIndexes = sequencesIndexes;
    }

    @Override
    public Iterator<Tuple2<Integer, List<LocationBasic>>> call(Sequence inputSequence){

        //public Iterable<Integer,Location> call(Sequence inputSequence) {

        ArrayList<Tuple2<Integer, List<LocationBasic>>> returnedValues = new ArrayList<Tuple2<Integer, List<LocationBasic>>>();

        int currentStart = 0;
        int currentEnd = MCSConfiguration.windowSize;

        //String currentWindow = "";
        //StringBuffer currentWindow = new StringBuffer();
        int numWindows = 0;
        int current_sketch_size = MCSConfiguration.sketchSize;

        String kmer;
        String reversed_kmer;
        int kmer32;

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

                // Compute k-mers
                kmer = "";
                kmer32 = 0;
                //LOG.warn("[JMAbuin] Current window is: " + currentWindow);

                // We compute the k-mers. In C
                int sketchValues[] = HashFunctions.window2sketch32(inputSequence.getData().substring(currentStart, currentEnd)
                        , current_sketch_size, MCSConfiguration.kmerSize);

                if (sketchValues != null) {
                    //LOG.warn("[JMAbuin] CurrentWindow sketch size: " + sketchValues.length);

                    for (int newValue : sketchValues) {

                        //LOG.warn("Calculated value: " + newValue);
                        List<LocationBasic> new_list = new ArrayList<>();
                        new_list.add(new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()).intValue(), numWindows));
                        returnedValues.add(new Tuple2<Integer, List<LocationBasic>>(newValue, new_list));

                        //returnedValues.add(new Tuple2<Integer, LocationBasic>(this.sequencesIndexes.get(inputSequence.getSeqId()).intValue(),
                        //                new LocationBasic(newValue, numWindows)));

                    }
                }


            }
            numWindows++;
            currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
            currentEnd = currentStart + MCSConfiguration.windowSize;

        }
        //LOG.warn("[JMAbuin] Total windows: "+numWindows);

        //}


        return returnedValues.iterator();
    }





}
