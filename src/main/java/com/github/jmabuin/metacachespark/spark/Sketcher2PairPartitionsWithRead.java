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
import com.github.jmabuin.metacachespark.io.ExtractionFunctions;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by chema on 1/16/17.
 */
//public class Sketcher2Pair implements FlatMapFunction<Iterator<Sequence>,HashMap<Integer, ArrayList<LocationBasic>>> {
public class Sketcher2PairPartitionsWithRead implements PairFlatMapFunction<Iterator<Sequence>,Integer, LocationBasic> {
    private static final Log LOG = LogFactory.getLog(Sketcher2Pair.class);

    private TreeMap<String, Integer> sequencesIndexes;
    private List<String> filenames;

    public Sketcher2PairPartitionsWithRead(TreeMap<String, Integer> sequencesIndexes, List<String> filenames) {
        //super();
        this.sequencesIndexes = sequencesIndexes;
        this.filenames = filenames;
    }

    @Override
    public Iterator<Tuple2<Integer, LocationBasic>> call(Iterator<Sequence> inputSequences){

        //public Iterable<Integer,Location> call(Sequence inputSequence) {

        ArrayList<Tuple2<Integer, LocationBasic>> returnedValues = new ArrayList<Tuple2<Integer, LocationBasic>>();
        HashMap<String, Sequence> my_sequences = new HashMap<>();

        while(inputSequences.hasNext()) {

            Sequence current_sequence = inputSequences.next();

            my_sequences.put(current_sequence.getHeader(), current_sequence);


        }

        try {


            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            //long sequence_number = 0;

            ExtractionFunctions extraction = new ExtractionFunctions();

            StringBuffer header = new StringBuffer();
            //StringBuffer data = new StringBuffer();
            StringBuffer content = new StringBuffer();

            boolean process_current_sequence =false;

            for (String current_file : this.filenames) {


                //if(!fileName.contains("assembly_summary")) {
                if (this.isSequenceFilename(current_file)) {

                    //LOG.warn("Start of Reading file: "+current_file);

                    String key = current_file;
                    FSDataInputStream inputStream = fs.open(new Path(current_file));

                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

                    String currentLine;

                    Sequence current_sequence = null;

                    while ((currentLine = br.readLine()) != null) {

                        if (currentLine.startsWith(">")) {
                            if (!header.toString().isEmpty()) {

                                if (!content.toString().isEmpty()) {
                                    // Process this sequence
                                    current_sequence = my_sequences.get(header.toString());
                                    current_sequence.setData(content.toString());

                                    this.process_sequence(current_sequence, returnedValues);
                                    //current_sequence.reset_data();
                                    content.delete(0, content.length());

                                }

                            }

                            header.delete(0, header.length());
                            header.append(currentLine.substring(1));
                            //data = "";
                            //data.delete(0,data.length());

                            if (my_sequences.containsKey(header.toString())) {

                                process_current_sequence = true;

                            }
                            else {
                                process_current_sequence = false;
                            }


                        }
                        else if (process_current_sequence) {
                            content.append(currentLine);
                        }


                    }

                    br.close();
                    inputStream.close();

                    //LOG.warn("End of Reading file: "+current_file);

                    if ((!content.toString().isEmpty()) && (!header.toString().isEmpty())) {
                        //Process last sequence from this file
                        current_sequence = my_sequences.get(header.toString());
                        current_sequence.setData(content.toString());

                        this.process_sequence(current_sequence, returnedValues);
                        //current_sequence.reset_data();
                        content.delete(0, content.length());
                    }


                }


            }

        }
        catch(IOException e) {
            LOG.error("Could not acces to HDFS");
            System.exit(-1);
        }


        //LOG.warn("[JMAbuin] Total windows: "+numWindows);

        //}


        return returnedValues.iterator();
    }


    private boolean isSequenceFilename(String filename) {

        if (filename.endsWith(".fna") || filename.endsWith(".fa") || filename.endsWith(".fq") || filename.endsWith(".fastq") || filename.endsWith(".fasta")){
            return true;
        }
        return false;


    }


    private void process_sequence(Sequence inputSequence, ArrayList<Tuple2<Integer, LocationBasic>> returnedValues) {
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
                        returnedValues.add(new Tuple2<Integer, LocationBasic>(newValue,
                                new LocationBasic(this.sequencesIndexes.get(inputSequence.getSeqId()), numWindows)));


                    }
                }


            }
            numWindows++;
            currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
            currentEnd = currentStart + MCSConfiguration.windowSize;

        }

    }

}
