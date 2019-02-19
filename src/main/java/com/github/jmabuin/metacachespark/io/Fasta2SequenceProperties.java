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

package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.Sequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/28/17.
 */
public class Fasta2SequenceProperties implements PairFlatMapFunction<Iterator<String>, String, Long> {


    private static final Log LOG = LogFactory.getLog(Fasta2SequenceProperties.class);

    @Override
    public Iterator<Tuple2<String, Long>> call(Iterator<String> fileNames) {

        List<Tuple2<String, Long>> returnValues = new ArrayList<Tuple2<String, Long>>();

        StringBuffer header = new StringBuffer();
        Long length = 0L;

        try {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            //long sequence_number = 0;

            ExtractionFunctions extraction = new ExtractionFunctions();

            while(fileNames.hasNext()) {


                String fileName = fileNames.next();

                //if(!fileName.contains("assembly_summary")) {
                if(this.isSequenceFilename(fileName)) {

                    FSDataInputStream inputStream = fs.open(new Path(fileName));

                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

                    String currentLine;

                    Sequence current_sequence = null;

                    while ((currentLine = br.readLine()) != null) {

                        if (currentLine.startsWith(">")) {
                            if(!header.toString().isEmpty()) {

                                returnValues.add(new Tuple2<String, Long>(header.toString(), length));

                                //sequence_number++;
                            }

                            header.delete(0,header.length());
                            header.append(currentLine.substring(1));
                            length = 0L;

                        }
                        else {

                            length += currentLine.length();

                        }

                    }

                    br.close();
                    inputStream.close();

                    //LOG.warn("Reading file: "+fileName+" - " + content.length());

                    if (!header.toString().isEmpty()) {
                        returnValues.add(new Tuple2<String, Long>(header.toString(), length));

                    }



                }


            }


            return returnValues.iterator();
        }
        catch(IOException e) {
            LOG.error("Could not acces to HDFS");
            System.exit(-1);
        }


        return returnValues.iterator();

    }



    private boolean isSequenceFilename(String filename) {

        if (filename.endsWith(".fna") || filename.endsWith(".fa") || filename.endsWith(".fq") || filename.endsWith(".fastq") || filename.endsWith(".fasta")){
            return true;
        }
        return false;


    }

}
