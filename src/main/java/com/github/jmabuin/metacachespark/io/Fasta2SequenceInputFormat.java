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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.bdgenomics.formats.avro.AlignmentRecord;
import scala.Function1;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by chema on 3/28/17.
 */
public class Fasta2SequenceInputFormat implements Function<PartialSequence, Sequence> {


    private static final Log LOG = LogFactory.getLog(Fasta2Sequence.class);
    private HashMap<String, Integer> targets_positions;

    public Fasta2SequenceInputFormat(HashMap<String, Integer> targets_positions) {
        this.targets_positions = targets_positions;
    }

    @Override
    public Sequence call(PartialSequence r1) {


        ExtractionFunctions extraction = new ExtractionFunctions();

        String header = r1.getHeader();//partial_sequence.toString().replace(">", "");
        String otherheader = r1.getKey();
        LOG.warn("Header is: " + header + ", and other is: " + otherheader);

        String value = r1.getValue();

        long index = this.targets_positions.get(header);


        Sequence currentSequence = new Sequence(index, header, r1.getValue(), "", "");



        String seqId = extraction.extract_sequence_id(currentSequence.getHeader());//SequenceReader.extract_sequence_id(currentSequence.getHeader());
        String fileIdentifier = extraction.extract_sequence_id(FilenameUtils.getName(currentSequence.getOriginFilename()));//SequenceReader.extract_sequence_id(fileName);

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
                        /*int taxid = 0;

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

                        currentSequence.setTaxid(taxid);*/
        currentSequence.getSequenceOrigin().setIndex(this.targets_positions.get(currentSequence.getHeader()));
        //currentSequence.getSequenceOrigin().setFilename(fileName);
        currentSequence.setSeqId(seqId);

        //currentIndexNumber++;

        return currentSequence;


    }

    private boolean isSequenceFilename(String filename) {

        if (filename.endsWith(".fna") || filename.endsWith(".fa") || filename.endsWith(".fq") || filename.endsWith(".fastq") || filename.endsWith(".fasta")){
            return true;
        }
        return false;


    }

}
