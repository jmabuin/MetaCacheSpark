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
package com.github.jmabuin.metacachespark.spark;
//import org.apache.spark.api.java.function.Function2;
import com.github.jmabuin.metacachespark.Build;
import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.*;

/**
 * Class to read FASTA files from HDFS and store results directly in a RDD of Sequence objects
 */
public class FastqSequenceReader extends SequenceReader implements FlatMapFunction<Tuple2<String, String>, Sequence> {

    private HashMap<String, Long> sequ2taxid;
    private Build.build_info infoMode;

    private static final Log LOG = LogFactory.getLog(FastaSequenceReader.class);
    private int currentMaxValue = Integer.MAX_VALUE;

    //private HashMap<String, Integer> sequenceIndex;

    //public FastaSequenceReader(HashMap<String, Long> sequ2taxid, Build.build_info infoMode, HashMap<String, Integer> sequenceIndex){
    public FastqSequenceReader(HashMap<String, Long> sequ2taxid, Build.build_info infoMode){
        //LOG.warn("[JMAbuin] Creating FastaSequenceReader object ");
        super();
        this.sequ2taxid = sequ2taxid;
        this.infoMode = infoMode;

        //this.sequenceIndex = sequenceIndex;
    }

    @Override
    public Iterator<Sequence> call(Tuple2<String, String> arg0) {
        StringBuffer header = new StringBuffer();
        StringBuffer data = new StringBuffer();
        StringBuffer quality = new StringBuffer();

        StringBuffer currentInput = new StringBuffer();
        StringBuffer currentFile = new StringBuffer();

        int fileId = 0;

        ArrayList<Sequence> returnedValues = new ArrayList<Sequence>();

        Tuple2<String, String> currentItem;

        //while(arg0.hasNext()) {

        currentInput.delete(0, currentInput.length());
        currentFile.delete(0, currentFile.length());

        //currentItem = arg0.next();
        currentInput.append(arg0._2());
        currentFile.append(arg0._1());

        if(!currentInput.toString().startsWith(">")) {
            return returnedValues.iterator();
        }

        long sequence_number = 0;
        //long line_number= 0;

        String lines_in_file[] = currentInput.toString().split("\n");

        //for (String newLine : currentInput.toString().split("\n")) {
        //while(line_number < lines_in_file.length) {
        for(int i = 0; i< lines_in_file.length; i+=4) {

            if (lines_in_file[i].startsWith("@")) {
                header.append(lines_in_file[i].substring(1));
                data.append(lines_in_file[i + 1]);
                quality.append(lines_in_file[i + 3]);

                returnedValues.add(new Sequence(sequence_number, header.toString(), data.toString(), quality.toString()));
                sequence_number++;

            }


        }

        int currentIndexNumber = 0;

        for (Sequence currentSequence : returnedValues) {
            //LOG.info("Processing file: "+ currentFile);


            String seqId = SequenceReader.extract_sequence_id(currentSequence.getHeader());
            String fileIdentifier = SequenceReader.extract_sequence_id(currentFile.toString());

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
            if (taxid > 0) {
                if (infoMode == Build.build_info.verbose)
                    LOG.info("[" + seqId + ":" + taxid + "] ");
            } else {
                taxid = SequenceReader.extract_taxon_id(currentSequence.getHeader()).intValue();
                if (infoMode == Build.build_info.verbose)
                    LOG.info("[" + seqId + "] ");
            }

            currentSequence.setTaxid(taxid);
            currentSequence.getSequenceOrigin().setIndex(currentIndexNumber);
            currentSequence.getSequenceOrigin().setFilename(currentFile.toString());
            currentSequence.setSeqId(seqId);

            currentIndexNumber++;
        }


        //}

        return returnedValues.iterator();
    }


}