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
 * Created by Jose M. Abuin on 3/28/17.
 */
public class PartialQueryNativeTreeMapBest implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNativeTreeMapBest.class);

    private String fileName;
    private long init;
    private int bufferSize;
    private long total;
    private SequenceFileReaderNative seqReader;
    private long readed;
    private String local_file_name;
    private int result_size;

    public PartialQueryNativeTreeMapBest(String file_name, long init, int bufferSize, long total, long readed, int result_size) {
        this.fileName = file_name;
        this.init = init;
        this.bufferSize = bufferSize;
        this.total = total;
        this.readed = readed;
        this.result_size = result_size;

    }

    @Override
    public Iterator<Tuple2<Long, TreeMap<LocationBasic, Integer>>> call(Iterator<HashMultiMapNative> myHashMaps) {

        List<Tuple2<Long, TreeMap<LocationBasic, Integer>>> finalResults = new ArrayList<Tuple2<Long, TreeMap<LocationBasic, Integer>>>();

        try{

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            Path hdfs_file_path = new Path(this.fileName);
            Path local_file_path = new Path(hdfs_file_path.getName());

            File tmp_file = new File(local_file_path.getName());

            if(!tmp_file.exists()){
                fs.copyToLocalFile(hdfs_file_path, local_file_path);
                LOG.info("File " + local_file_path.getName() + " copied");
            }
            else {
                LOG.info("File " + local_file_path.getName() + " already exists. Not copying.");
            }

            this.local_file_name = local_file_path.getName();
            this.seqReader = new SequenceFileReaderNative(this.local_file_name);

            if (this.init!=0) {
                this.seqReader.skip(this.init);
            }

            ArrayList<Sketch> locations = new ArrayList<Sketch>();

            List<SequenceData> inputData = new ArrayList<>();
            long currentSequence = this.init;

            //LOG.warn("Init at sequence: " + currentSequence);

            // Theoretically there is only one HashMap per partition
            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                LOG.info("Processing hashmap " + currentSequence );
                //for(SequenceData currentData: inputData){

                LocationBasic loc = new LocationBasic();
                HashMap<LocationBasic, Integer> all_hits = new HashMap<>();
                //TreeMap<Integer, LocationBasic> all_hits_sorted = new TreeMap<>();

                while((this.seqReader.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                    String header = this.seqReader.get_header();
                    String data = this.seqReader.get_data();
                    String qua = this.seqReader.get_quality();

                    if (this.seqReader.get_header().isEmpty()) {
                        continue;
                    }

                    TreeMap<LocationBasic, Integer> current_results = new TreeMap<>();



                    //if ((currentSequence == this.init) || (currentSequence >= (this.init + this.bufferSize-5))) {
                    if (currentSequence == this.init) {
                        LOG.warn("Processing sequence " + currentSequence + " :: " + header);
                    }


                    SequenceData currentData = new SequenceData(header, data, qua);

                    // LOG.warn("Getting sketch");
                    locations = SequenceFileReader.getSketchStatic(currentData);
                    //LOG.warn("Sketch acquired ");
                    //LOG.warn("Sketch acquired " + locations.size());
                    //if(locations == null) {
                    //    LOG.warn("Locations is null!!");
                    //}

                    all_hits.clear();
                    //all_hits_sorted.clear();

                    int block_size = locations.size() * this.result_size;

                    for(Sketch currentSketch: locations) {
                        //LOG.warn("Current sketch");
                        //LOG.warn("Current sketch: " + currentSketch.getFeatures().length);
                        //all_hits.clear();

                        for(int location: currentSketch.getFeatures()) {
                            //LOG.warn("getting items from database");

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);


                            if(locations_obtained != null) {
                                //LOG.warn("Values received");
                                //LOG.warn("Size of received values is: " + values.length);


                                //for (LocationBasic current_loc: locations_obtained) {
                                for (int i = 0; i< locations_obtained.length; ++i){

                                    //if (locations_obtained[i].getTargetId() < 0 ) {
                                    //    LOG.warn("Before sorting Location: " + locations_obtained[i].getTargetId() + "::" + locations_obtained[i].getWindowId());
                                    //}

                                    if (all_hits.containsKey(locations_obtained[i])) {
                                        all_hits.put(locations_obtained[i], all_hits.get(locations_obtained[i]) + 1);
                                    }
                                    else{
                                        all_hits.put(locations_obtained[i], 1);
                                    }
                                    /*
                                    if (current_results.containsKey(current_loc)) {
                                        current_results.put(current_loc, current_results.get(current_loc) + 1);
                                    }
                                    else {

                                        current_results.put(current_loc, 1);
                                    }
                                    */
                                }

                                //LOG.warn("Locations added " + current_results.size());


                            }
                        }



                        //LOG.warn("Size when quering is: " + current_results.size());


                    }

                    if (!all_hits.isEmpty()) {
/*
                            for(LocationBasic key: all_hits.keySet()) {
                                all_hits_sorted.put(all_hits.get(key), key);
                            }

                            int current_number_of_values = 0;

                            Set<Integer> key_set = all_hits_sorted.descendingMap().keySet();

                            for(Integer current_key:key_set) {
                                if (all_hits_sorted.get(current_key).getTargetId() < 0) {
                                    LOG.warn("After sorting Location: " + all_hits_sorted.get(current_key).getTargetId() + "::" + all_hits_sorted.get(current_key).getWindowId());
                                }

                                current_results.put(all_hits_sorted.get(current_key), current_key);
                                current_number_of_values++;

                                if(current_number_of_values >= this.result_size) {
                                    break;
                                }

                            }
*/

                        List<Map.Entry<LocationBasic, Integer>> list = new ArrayList<>(all_hits.entrySet());
                        list.sort(Map.Entry.comparingByValue());
                        //Collections.reverse(list);
                        //LOG.warn("Size of hashmap: " + all_hits.size() + ", size of list: " + list.size());
                        //for(Map.Entry<LocationBasic, Integer> current_entry : list) {
                        for (int i = list.size() - 1, current_number_of_values = 0; (current_number_of_values < this.result_size) && (i >= 0); --i, ++current_number_of_values) {
                            Map.Entry<LocationBasic, Integer> current_entry = list.get(i);
                            //if (current_entry.getKey().getTargetId() < 0) {
                            //    LOG.warn("After sorting Location: " + current_entry.getKey().getTargetId() + "::" + current_entry.getKey().getWindowId());
                            // }
                            if (current_results.containsKey(current_entry.getKey())) {
                                current_results.put(current_entry.getKey(), current_results.get(current_entry.getKey()) + current_entry.getValue());
                            } else {
                                current_results.put(current_entry.getKey(), current_entry.getValue());
                            }

                        }

                    }


                    //LOG.warn("Adding results");
                    finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(currentSequence, current_results));
                    //LOG.warn("Results added");
                    //data = seqReader.next();
                    currentSequence++;
                    locations.clear();
                }

                this.seqReader.close();
            }

            //LOG.warn("Ending buffer " + currentSequence);

            return finalResults.iterator();


        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativeTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}