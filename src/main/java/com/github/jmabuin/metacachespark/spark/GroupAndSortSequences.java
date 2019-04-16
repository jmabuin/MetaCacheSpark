package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.io.ExtractionFunctions;
import com.github.jmabuin.metacachespark.io.Fasta2Sequence;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class GroupAndSortSequences implements Function<Tuple2<String, Iterable<Tuple2<Long, String>>>, Sequence> {

    private static final Log LOG = LogFactory.getLog(GroupAndSortSequences.class);
    private HashMap<String, Integer> targets_positions;


    public GroupAndSortSequences(HashMap<String, Integer> targets_positions) {
        this.targets_positions = targets_positions;
    }
    @Override
    public Sequence call(Tuple2<String, Iterable<Tuple2<Long, String>>> input) {

        String file_name = input._1;

        List<Tuple2<Long, String>> input_list = Lists.newArrayList(input._2);

        input_list.sort(new Comparator<Tuple2<Long, String>>() {
            public int compare(Tuple2<Long, String> o1,
                               Tuple2<Long, String> o2)
            {

                if (o1._1 > o2._1) {
                    return 1;
                }

                if (o1._1 < o2._1) {
                    return -1;
                }

                return 0;

            }
        });


        if (input_list.size()==0) {
            LOG.error("List size is zero!!");
            return null;
        }

        if (!input_list.get(0)._2.startsWith(">")) {
            LOG.error("The FASTA sequence does not starts with the correct character > !!");
            return null;
        }


        ExtractionFunctions extraction = new ExtractionFunctions();

        String[] sequence_items_first = input_list.get(0)._2.split("\n");

        String header = sequence_items_first[0].replace(">", "");

        StringBuilder str_builder = new StringBuilder();

        for(int i = 1; i< sequence_items_first.length; ++i) {
            str_builder.append(sequence_items_first[i]);

        }

        for(int i = 1; i< input_list.size(); ++i) {

            str_builder.append(input_list.get(i)._2.replace("\n", ""));

        }

        long index = this.targets_positions.get(header);


        Sequence currentSequence = new Sequence(index, header, str_builder.toString(), "", file_name);



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


}
