package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by chema on 3/28/17.
 */
public class PartialQueryNative implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, HashMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNative.class);

    private String fileName;
    private long init;
    private int bufferSize;
    private long total;
    private SequenceFileReaderNative seqReader;
    private long readed;
    private String local_file_name;

    public PartialQueryNative(String file_name, long init, int bufferSize, long total, long readed) {
        //this.seqReader = seqReader;
        this.fileName = file_name;
        this.init = init;
        this.bufferSize = bufferSize;
        this.total = total;
        this.readed = readed;

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
            this.seqReader = new SequenceFileReaderNative(this.local_file_name);

            if (this.init!=0) {
                this.seqReader.skip(this.init);
            }

            ArrayList<Sketch> locations = new ArrayList<Sketch>();

            //HashMap<String, List<int[]>> results = new HashMap<String, List<int[]>>();

            //finalResults.add(results);
            List<SequenceData> inputData = new ArrayList<>();
            long currentSequence = this.init;

            //LOG.warn("[JMAbuin] Init at sequence: " + currentSequence);
            // Theoretically there is only one HashMap per partition
            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                LOG.info("Processing hashmap " + currentSequence );
                //for(SequenceData currentData: inputData){
                while((this.seqReader.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                    HashMap<LocationBasic, Integer> current_results = new HashMap<>();

                    String header = this.seqReader.get_header();
                    String data = this.seqReader.get_data();
                    String qua = this.seqReader.get_quality();

                    //LOG.info("Processing sequence " + currentSequence + " :: " + data);

                    SequenceData currentData = new SequenceData(header, data, qua);

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

                    finalResults.add(new Tuple2<Long, HashMap<LocationBasic, Integer>>(currentSequence, current_results));

                    //data = seqReader.next();
                    currentSequence++;
                    locations.clear();
                }
            }

            //LOG.warn("[JMAbuin] Ending buffer " + currentSequence);

            //seqReader.close();

            return finalResults.iterator();


        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNative: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}