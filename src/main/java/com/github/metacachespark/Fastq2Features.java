/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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
package com.github.metacachespark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.*;


public class Fastq2Features implements FlatMapFunction<Tuple2<Long, String>,Feature> {

    private HashMap<String, Long> sequ2taxid;
    private Build.build_info infoMode;

    private static final Log LOG = LogFactory.getLog(Fastq2Features.class);
    //private int currentMaxValue = Integer.MAX_VALUE;

    public Fastq2Features(HashMap<String, Long> sequ2taxid, Build.build_info infoMode){
        //LOG.warn("[JMAbuin] Creating FastaSequenceReader object ");
        this.sequ2taxid = sequ2taxid;
        this.infoMode = infoMode;
    }

    @Override
    public Iterable<Feature> call(Tuple2<Long, String> arg0) {
        //LOG.warn("[JMAbuin] Starting Call function");
        //String header = "";
        StringBuffer header = new StringBuffer();
        StringBuffer data = new StringBuffer();

        String currentInput = arg0._2();
        Long currentRead = arg0._1();

        int fileId = 0;
        ArrayList<Feature> returnedValues = new ArrayList<Feature>();
        ArrayList<Sequence> sequences = new ArrayList<Sequence>();


            int currentStart = 0;
            int currentEnd = MCSConfiguration.windowSize;



            //ArrayList<Feature> returnedValues = new ArrayList<Feature>();

            String currentWindow = "";
            int numWindows = 0;


            // We iterate over windows (with overlap)
            while (currentEnd < currentInput.length()) {
                //Sketch resultSketch = new Sketch();

                currentWindow = currentInput.substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

                // Compute k-mers
                // We compute the k-mers. In C
                int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

                for(int newValue: sketchValues) {

                    //returnedValues.add(new Feature(newValue, currentSequence.getPartitionId(), currentSequence.getFileId(), currentSequence.getHeader(), currentSequence.getTaxid()));
                    returnedValues.add(new Feature(newValue, 0, 0, numWindows));

                }


                numWindows++;
                currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
                currentEnd = currentStart + MCSConfiguration.windowSize;

            }



        //endTime = System.nanoTime();
        //LOG.warn("Time for file "+currentFile+" is: " + ((endTime - initTime)/1e9));
        return returnedValues;
    }

}
