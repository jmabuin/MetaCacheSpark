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
public class Fasta2SequencePair implements PairFlatMapFunction<Iterator<String>, Long, Sequence> {
    //public class Fasta2SequencePair implements PairFlatMapFunction<Iterator<Tuple2<String, Long>>, Long, Sequence> {


    private static final Log LOG = LogFactory.getLog(Fasta2SequencePair.class);
    private HashMap<String, Long> sequ2taxid;
    private HashMap<String, Integer> targets_positions;

    public Fasta2SequencePair(HashMap<String, Long> sequ2taxid, HashMap<String, Integer> targets_positions) {
        this.sequ2taxid = sequ2taxid;
        this.targets_positions = targets_positions;
    }

    @Override
    public Iterator<Tuple2<Long, Sequence>> call(Iterator<String> fileNames) {

        List<Tuple2<Long, Sequence>> returnValues = new ArrayList<Tuple2<Long, Sequence>>();

        StringBuilder header = new StringBuilder();
        //StringBuffer data = new StringBuffer();
        StringBuilder content = new StringBuilder();

        try {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            //long sequence_number = 0;

            ExtractionFunctions extraction = new ExtractionFunctions();

            int current_filename = 0;
            int current_sequence_number = 0;

            while(fileNames.hasNext()) {


                content.delete(0, content.toString().length());

                String fileName = fileNames.next();


                //if(!fileName.contains("assembly_summary")) {
                if (this.isSequenceFilename(fileName)) {

                    String key = fileName;

                    current_filename++;
                    //LOG.warn("Start of Reading file: "+fileName);
                    FSDataInputStream inputStream = fs.open(new Path(fileName));

                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

                    String currentLine;

                    Sequence current_sequence = null;

                    while ((currentLine = br.readLine()) != null) {

                        if (currentLine.startsWith(">")) {


                            if (!header.toString().isEmpty()) {
                                //returnedValues.add(new Sequence(data.toString(), "", currentFile.toString(), -1,
                                //		header.toString(), -1));
                                //LOG.warn("Adding sequence : " + header.toString());
                                long index = this.targets_positions.get(header.toString());

                                //if (current_sequence != null) {
                                returnValues.add(new Tuple2<Long, Sequence>(index, new Sequence(index, header.toString(), content.toString(), "", fileName)));
                                current_sequence_number++;

                                //}


                                //sequence_number++;
                            }

                            header.delete(0, header.length());
                            header.append(currentLine.substring(1));
                            //data = "";
                            //data.delete(0,data.length());
                            content.delete(0, content.length());


                        } else {

                            //data.append(currentLine);
                            //current_sequence.appendData(currentLine);
                            content.append(currentLine);

                        }

                    }

                    br.close();
                    inputStream.close();


                    //LOG.warn("End of reading file: "+fileName);

                    //if ((!current_sequence.getData().isEmpty()) && (!header.toString().isEmpty())) {
                    if ((!header.toString().isEmpty()) && (!content.toString().isEmpty())) {
                        //LOG.warn("Adding last sequence : " + header.toString() + " from file " + fileName);
                        long index = this.targets_positions.get(header.toString());
                        returnValues.add(new Tuple2<Long, Sequence>(index, new Sequence(index, header.toString(), content.toString(), "", fileName)));
                        current_sequence_number++;

                    }
                    header.delete(0, header.length());
                    content.delete(0, content.length());

                }
            }
                    //int currentIndexNumber = 0;

                    for (Tuple2<Long, Sequence> currentSequence_tuple : returnValues) {
                        //LOG.info("Processing file: "+ currentFile);

                        Sequence currentSequence = currentSequence_tuple._2;

                        String seqId = extraction.extract_sequence_id(currentSequence.getHeader());//SequenceReader.extract_sequence_id(currentSequence.getHeader());
                        String fileIdentifier = extraction.extract_sequence_id(currentSequence.getOriginFilename());//SequenceReader.extract_sequence_id(fileName);

                        //make sure sequence id is not empty,
                        //use entire header if neccessary
                        if (seqId.isEmpty()) {
                            if (!fileIdentifier.isEmpty()) {
                                seqId = fileIdentifier;
                            } else {
                                seqId = currentSequence.getHeader();
                            }
                        }

                        //targets need to have a sequence id
                        //look up taxon id
                        int taxid = 0;

                        if (!sequ2taxid.isEmpty()) {
                            Long it = sequ2taxid.get(seqId);
                            if (it != null) {
                                taxid = it.intValue();
                            } else {
                                it = sequ2taxid.get(fileIdentifier);
                                if (it != null) {
                                    taxid = it.intValue();
                                }
                            }
                        }
                        //no valid taxid assigned -> try to find one in annotation

                        if (taxid <= 0) {
                            taxid = SequenceReader.extract_taxon_id(currentSequence.getHeader()).intValue();

                        }

                        currentSequence.setTaxid(taxid);
                        currentSequence.getSequenceOrigin().setIndex(this.targets_positions.get(currentSequence.getHeader()));
                        //currentSequence.getSequenceOrigin().setFilename(fileName);
                        currentSequence.setSeqId(seqId);

                        //currentIndexNumber++;
                    }




            LOG.warn("Processed files: " + current_filename + ", processed sequences: " + current_sequence_number);

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
