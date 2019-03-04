package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.io.ExtractionFunctions;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class CreateDatabaseNative implements FlatMapFunction<Iterator<Integer>, HashMultiMapNative> {

    private static final Log LOG = LogFactory.getLog(CreateDatabaseNative.class);
    //private HashMap<String, Integer> sequences_distribution;
    private List<String> input_files;
    //private HashMap<String, Integer> targets_positions;
    private ExtractionFunctions extraction;
    private TreeMap<String, Long> sequ2taxid;
    private TreeMap<String, Long> name2tax_;
    private int total_executors;

    public CreateDatabaseNative(//HashMap<String, Integer> sequences_distribution,
                                List<String> input_files,
                                //HashMap<String, Integer> targets_positions,
                                TreeMap<String, Long> sequ2taxid,
                                TreeMap<String, Long> name2tax_,
                                int total_executors) {

        //this.sequences_distribution = sequences_distribution;
        this.input_files = input_files;
        //this.targets_positions = targets_positions;
        this.extraction = new ExtractionFunctions();
        this.sequ2taxid = sequ2taxid;
        this.name2tax_ = name2tax_;
        this.total_executors = total_executors;
    }

    @Override
    public Iterator<HashMultiMapNative> call(Iterator<Integer> executor_id){

        ArrayList<HashMultiMapNative> returnedValues = new ArrayList<HashMultiMapNative>();
        HashMultiMapNative map = new HashMultiMapNative();

        //long sequence_number = 0;



        Integer this_executor = -1;

        //LOG.info("Starting final step");
        while (executor_id.hasNext()) {
            this_executor = executor_id.next();
        }

        //LOG.info("I am executor: " + this_executor);
        StringBuilder header = new StringBuilder();
        //StringBuffer data = new StringBuffer();
        StringBuilder content = new StringBuilder();

        int current_filename = 0;
        int current_sequence_number = 0;

        try {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            for (String fileName : input_files) {

                content.delete(0, content.toString().length());

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

                                long index = current_sequence_number;//this.targets_positions.get(header.toString());

                                //returnValues.add(new Tuple2<Long, Sequence>(index, new Sequence(index, header.toString(), content.toString(), "", fileName)));
                                // Create current sequence. Process it and insert values into HashMap

                                //if (this.sequences_distribution.get(header.toString()) == this_executor) {
                                if (index % this.total_executors == this_executor) {
                                    current_sequence = new Sequence(index, header.toString(), content.toString(), "", fileName);
                                    this.rank_sequence(current_sequence);
                                    this.sketcher(current_sequence, map);
                                }


                                current_sequence_number++;


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
                        long index = current_sequence_number;//this.targets_positions.get(header.toString());
                        //returnValues.add(new Tuple2<Long, Sequence>(index, new Sequence(index, header.toString(), content.toString(), "", fileName)));
                        // Create current sequence. Process it and insert values into HashMap
                        //if (this.sequences_distribution.get(header.toString()) == this_executor) {
                        if (index % this.total_executors == this_executor) {
                            current_sequence = new Sequence(index, header.toString(), content.toString(), "", fileName);
                            this.rank_sequence(current_sequence);
                            this.sketcher(current_sequence, map);
                        }
                        current_sequence_number++;

                    }
                    header.delete(0, header.length());
                    content.delete(0, content.length());

                }


            }
        }
        catch(IOException e) {
            LOG.error("Could not acces to HDFS");
            System.exit(-1);
        }

        returnedValues.add(map);

        return returnedValues.iterator();



    }

    private boolean isSequenceFilename(String filename) {

        if (filename.endsWith(".fna") || filename.endsWith(".fa") || filename.endsWith(".fq") || filename.endsWith(".fastq") || filename.endsWith(".fasta")){
            return true;
        }
        return false;


    }


    private void rank_sequence(Sequence current_Sequence) {
        String seqId = this.extraction.extract_sequence_id(current_Sequence.getHeader());//SequenceReader.extract_sequence_id(currentSequence.getHeader());
        String fileIdentifier = this.extraction.extract_sequence_id(current_Sequence.getOriginFilename());//SequenceReader.extract_sequence_id(fileName);

        //make sure sequence id is not empty,
        //use entire header if neccessary
        if (seqId.isEmpty()) {
            if (!fileIdentifier.isEmpty()) {
                seqId = fileIdentifier;
            } else {
                seqId = current_Sequence.getHeader();
            }
        }

        //targets need to have a sequence id
        //look up taxon id
        int taxid = 0;

        if (!this.sequ2taxid.isEmpty()) {
            Long it = this.sequ2taxid.get(seqId);
            if (it != null) {
                taxid = it.intValue();
            } else {
                it = this.sequ2taxid.get(fileIdentifier);
                if (it != null) {
                    taxid = it.intValue();
                }
            }
        }
        //no valid taxid assigned -> try to find one in annotation

        if (taxid <= 0) {
            taxid = SequenceReader.extract_taxon_id(current_Sequence.getHeader()).intValue();

        }

        current_Sequence.setTaxid(taxid);
        current_Sequence.getSequenceOrigin().setIndex((int)current_Sequence.getIndex());
        //currentSequence.getSequenceOrigin().setFilename(fileName);
        current_Sequence.setSeqId(seqId);
    }


    public void sketcher(Sequence inputSequence, HashMultiMapNative map) {
        int currentStart = 0;
        int currentEnd = MCSConfiguration.windowSize;

        //String currentWindow = "";
        //StringBuffer currentWindow = new StringBuffer();
        int numWindows = 0;
        int current_sketch_size;

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
                        map.add(newValue, this.name2tax_.get(inputSequence.getSeqId()).intValue(), numWindows);

                    }
                }


            }
            numWindows++;
            currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
            currentEnd = currentStart + MCSConfiguration.windowSize;

        }


        int total_deleted = map.post_process(false, false);
        //LOG.warn("Number of items in this partial map is: " + map.size());
        //LOG.warn("Number of deleted features: " + total_deleted);


    }


}
