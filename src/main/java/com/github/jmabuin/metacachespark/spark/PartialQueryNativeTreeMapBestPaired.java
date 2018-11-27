package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.io.*;
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
public class PartialQueryNativeTreeMapBestPaired implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNativeTreeMapBestPaired.class);

    private String fileName;
    private String fileName2;
    private long init;
    private int bufferSize;
    private long total;
    private long readed;
    private int result_size;

    public PartialQueryNativeTreeMapBestPaired(String file_name, String file_name2, long init, int bufferSize, long total, long readed, int result_size) {
        this.fileName = file_name;
        this.fileName2 = file_name2;
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

            Path hdfs_file_path2 = new Path(this.fileName2);
            Path local_file_path2 = new Path(hdfs_file_path2.getName());

            File tmp_file2 = new File(local_file_path2.getName());

            if(!tmp_file2.exists()){
                fs.copyToLocalFile(hdfs_file_path2, local_file_path2);
                LOG.info("File " + local_file_path2.getName() + " copied");
            }
            else {
                LOG.info("File " + local_file_path2.getName() + " already exists. Not copying.");
            }

            String local_file_name = local_file_path.getName();
            SequenceFileReaderNative seqReader = new SequenceFileReaderNative(local_file_name);

            String local_file_name2 = local_file_path2.getName();
            SequenceFileReaderNative2 seqReader2 = new SequenceFileReaderNative2(local_file_name2);

            if (this.init!=0) {
                seqReader.skip(this.init);
                seqReader2.skip(this.init);
            }

            ArrayList<Sketch> locations = new ArrayList<Sketch>();
            ArrayList<Sketch> locations2 = new ArrayList<Sketch>();

            long currentSequence = this.init;

            //LOG.warn("Init at sequence: " + currentSequence);

            // Theoretically there is only one HashMap per partition
            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                // HashMap to store hits from the two sequences
                HashMap<LocationBasic, Integer> all_hits = new HashMap<>();
                //TreeMap<Integer, LocationBasic> all_hits_sorted = new TreeMap<>();

                while((seqReader.next() != null) && (seqReader2.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                    String header = seqReader.get_header();
                    String data = seqReader.get_data();
                    String qua = seqReader.get_quality();

                    String header2 = seqReader2.get_header();
                    String data2 = seqReader2.get_data();
                    String qua2 = seqReader2.get_quality();

                    if (seqReader.get_header().isEmpty() || seqReader2.get_header().isEmpty()) {
                        continue;
                    }

                    // TreeMap to store BEST results for this pair of sequences
                    TreeMap<LocationBasic, Integer> current_results = new TreeMap<>();

                    //if ((currentSequence == this.init) || (currentSequence >= (this.init + this.bufferSize-5))) {
                    if ((currentSequence == this.init) || (currentSequence == this.init + this.bufferSize - 1)) {
                        LOG.warn("Processing sequence " + currentSequence + " :: " + header + " :: " + header2);
                        LOG.warn(data);
                        LOG.warn(data2);
                    }

                    SequenceData currentData = new SequenceData(header, data, qua);
                    SequenceData currentData2 = new SequenceData(header2, data2, qua2);

                    //LOG.warn("getting sketches for "+ " :: " + header + " :: " + header2);
                    locations = SequenceFileReader.getSketchStatic(currentData);
                    locations2 = SequenceFileReader.getSketchStatic(currentData2);

                    all_hits.clear();

                    //int block_size = locations.size() * this.result_size;
                    //LOG.warn("Getting locations for "+ " :: " + header);
                    for(Sketch currentSketch: locations) {

                        for(int location: currentSketch.getFeatures()) {

                            LocationBasic[] locations_obtained = currentHashMap.get_locations(location);

                            if(locations_obtained != null) {

                                for (int i = 0; i< locations_obtained.length; ++i){

                                    if (all_hits.containsKey(locations_obtained[i])) {
                                        all_hits.put(locations_obtained[i], all_hits.get(locations_obtained[i]) + 1);
                                    }
                                    else{
                                        all_hits.put(locations_obtained[i], 1);
                                    }

                                }
                            }
                        }
                    }

                    //LOG.warn("Getting locations for "+ " :: " + header2);
                    for(Sketch currentSketch: locations2) {

                        for(int location: currentSketch.getFeatures()) {

                            LocationBasic[] locations_obtained = currentHashMap.get_locations(location);

                            if(locations_obtained != null) {

                                for (int i = 0; i< locations_obtained.length; ++i){

                                    if (all_hits.containsKey(locations_obtained[i])) {
                                        all_hits.put(locations_obtained[i], all_hits.get(locations_obtained[i]) + 1);
                                    }
                                    else{
                                        all_hits.put(locations_obtained[i], 1);
                                    }

                                }
                            }
                        }
                    }

                    if (!all_hits.isEmpty()) {

                        List<Map.Entry<LocationBasic, Integer>> list = new ArrayList<>(all_hits.entrySet());
                        list.sort(Map.Entry.comparingByValue());
                        //Collections.reverse(list);
                        //LOG.warn("Size of hashmap: " + all_hits.size() + ", size of list: " + list.size());
                        //for(Map.Entry<LocationBasic, Integer> current_entry : list) {
                        for (int i = list.size() - 1, current_number_of_values = 0; (current_number_of_values < this.result_size) && (i >= 0); --i, ++current_number_of_values) {
                            Map.Entry<LocationBasic, Integer> current_entry = list.get(i);

                            if (current_results.containsKey(current_entry.getKey())) {
                                current_results.put(current_entry.getKey(), current_results.get(current_entry.getKey()) + current_entry.getValue());
                            } else {
                                current_results.put(current_entry.getKey(), current_entry.getValue());
                            }

                        }

                    }

                    finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(currentSequence, current_results));

                    currentSequence++;
                    locations.clear();
                    locations2.clear();
                }

                LOG.warn("Ending process!!!!");
                seqReader.close();
                seqReader2.close();
            }

            //LOG.warn("Ending buffer " + currentSequence);

            return finalResults.iterator();


        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativeTreeMapBestPaired: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}