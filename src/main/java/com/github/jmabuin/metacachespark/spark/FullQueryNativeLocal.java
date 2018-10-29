package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.*;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.io.SequenceData;
import com.github.jmabuin.metacachespark.io.SequenceFileReader;
import com.github.jmabuin.metacachespark.io.SequenceFileReaderNative;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by chema on 3/28/17.
 */
public class FullQueryNativeLocal implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, HashMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(FullQueryNativeLocal.class);

    private String fileName;
    private String local_file_name = "";
    private SequenceFileReaderNative sequenceFileReaderNative;

    public FullQueryNativeLocal(String fileName) {

        this.fileName = fileName;

    }

    @Override
    public Iterator<Tuple2<Long, HashMap<LocationBasic, Integer>>> call(Iterator<HashMultiMapNative> myHashMaps) {

        List<Tuple2<Long, HashMap<LocationBasic, Integer>>> finalResults = new ArrayList<Tuple2<Long, HashMap<LocationBasic, Integer>>>();


        try{


            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            Path hdfs_file_path = new Path(this.fileName);
            Path local_file_path = new Path(hdfs_file_path.getName());

            File tmp_file = new File(local_file_path.getName());

            if(!tmp_file.exists()){
                fs.copyToLocalFile(hdfs_file_path, local_file_path);
                LOG.warn("File " + local_file_path.getName() + " copied");
            }
            else {
                LOG.warn("File " + local_file_path.getName() + " already exists. Not copying.");
            }

            this.local_file_name = local_file_path.getName();
            this.sequenceFileReaderNative = new SequenceFileReaderNative(this.local_file_name);


            ArrayList<Sketch> locations = new ArrayList<Sketch>();
            TreeMap<LocationBasic, Integer> matches;

            long totalReads = FilesysUtility.readsInFastaFile(this.fileName);
            long startRead = 0;


            // Theoretically there is only one HashMap per partition
            //long currentSequence = 0;
            LOG.info("Init at sequence: " + startRead);

            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                //SequenceFileReader seqReader = new SequenceFileReader(this.fileName, 0);

                //SequenceData currentData;

                //for(startRead = 0; startRead < totalReads; startRead++) {
                long current_sequence_number = 0;
                while(this.sequenceFileReaderNative.next() != null) {

                    HashMap<LocationBasic, Integer> current_results = new HashMap<>();

                    String header = this.sequenceFileReaderNative.get_header();
                    String data = this.sequenceFileReaderNative.get_data();
                    String quality = this.sequenceFileReaderNative.get_quality();

                    SequenceData currentData = new SequenceData(header, data, quality);
                    //LOG.info("Processing sequence " + current_sequence_number);


                    //if(currentSequence >= this.init) {

                    locations = SequenceFileReader.getSketchStatic(currentData);

                    List<int[]> queryResults = new ArrayList<int[]>();

                    for(Sketch currentSketch: locations) {

                        for(int location: currentSketch.getFeatures()) {

                            int[] values = currentHashMap.get(location);

                            if(values != null) {

                                LocationBasic new_location = new LocationBasic(values[0], values[1]);

                                if(current_results.containsKey(new_location)) {
                                    current_results.put(new_location, current_results.get(new_location) + 1);
                                }
                                else {
                                    current_results.put(new_location, 1);
                                }


                            }
                        }

                    }

                    //results.put(data.getHeader(), queryResults);
                    finalResults.add(new Tuple2<Long, HashMap<LocationBasic, Integer>>(current_sequence_number, current_results));
                    //}
                    ++current_sequence_number;
                }
                this.sequenceFileReaderNative.close();
                LOG.warn("[JMAbuin] Finished all sequences");
            }

            //LOG.warn("[JMAbuin] Ending buffer " + currentSequence);

            return finalResults.iterator();

        }
        catch (IOException e) {
            LOG.error("Could not read file "+ this.local_file_name+ " because of IO error in SequenceReader.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch(Exception e) {
            LOG.error("ERROR in FullQueryTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}