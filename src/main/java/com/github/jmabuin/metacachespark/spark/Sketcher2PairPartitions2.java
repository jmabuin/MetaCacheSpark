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
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
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
public class Sketcher2PairPartitions2 implements PairFlatMapFunction<Iterator<Sequence>, Integer, LocationBasic> {

    private static final Log LOG = LogFactory.getLog(Sketcher2PairPartitions2.class);

    private TreeMap<String, Long> sequencesIndexes;
    private int firstEmptyPosition;
    private MetaCacheOptions options;


    public Sketcher2PairPartitions2(TreeMap<String, Long> sequencesIndexes, MetaCacheOptions options) {

        this.sequencesIndexes = sequencesIndexes;
        this.options = options;
        //this.firstEmptyPosition = 0;
        //this.lookup = new HashMap<Integer, Integer>();
    }

    @Override
    public Iterator<Tuple2<Integer, LocationBasic>> call(Iterator<Sequence> inputSequences){


        ArrayList<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();
        //HashMultiMapNative map = new HashMultiMapNative(254);

        int currentStart;
        int currentEnd;

        //String currentWindow;
        int numWindows;
        int current_sketch_size;

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

							returnedValues.add(new Tuple2<Integer, LocationBasic>(newValue,
									new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()).intValue(), numWindows)));


                        }
                    }


                }
                numWindows++;
                currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
                currentEnd = currentStart + MCSConfiguration.windowSize;

            }

/*
			for(Tuple2<Integer, LocationBasic> current_loc: returnedValues_local) {

				map.add(current_loc._1(), current_loc._2().getTargetId(), current_loc._2().getWindowId());

			}

			returnedValues_local.clear();
*/

        }




        return returnedValues.iterator();
    }


}
