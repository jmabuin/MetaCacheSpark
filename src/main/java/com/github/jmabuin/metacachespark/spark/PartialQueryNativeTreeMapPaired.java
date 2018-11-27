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
public class PartialQueryNativeTreeMapPaired implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNativeTreeMapPaired.class);

    private String fileName;
    private String fileName2;
    private long init;
    private int bufferSize;
    private long total;
    private long readed;
    private int result_size;

    public PartialQueryNativeTreeMapPaired(String file_name, String file_name2, long init, int bufferSize, long total, long readed, int result_size) {
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
            SequenceFileReaderNative seqReader;
            SequenceFileReaderNative2 seqReader2;

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
            String local_file_name2 = local_file_path2.getName();

            seqReader = new SequenceFileReaderNative(local_file_name);
            seqReader2 = new SequenceFileReaderNative2(local_file_name2);

            if (this.init!=0) {
                seqReader.skip(this.init);
                seqReader2.skip(this.init);
            }

            ArrayList<Sketch> locations = new ArrayList<Sketch>();
            ArrayList<Sketch> locations2 = new ArrayList<Sketch>();

            long currentSequence = this.init;

            // Theoretically there is only one HashMap per partition
            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                //LOG.info("Processing hashmap " + currentSequence );

                LocationBasic loc = new LocationBasic();

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

                    // TreeMap where hits from this sequences will be stored
                    TreeMap<LocationBasic, Integer> current_results = new TreeMap<>();

                    if (currentSequence == this.init) {
                        LOG.warn("Processing sequence " + currentSequence + " :: " + header + " :: " + header2);
                    }


                    SequenceData currentData = new SequenceData(header, data, qua);
                    SequenceData currentData2 = new SequenceData(header2, data2, qua2);

                    locations = SequenceFileReader.getSketchStatic(currentData);
                    locations2 = SequenceFileReader.getSketchStatic(currentData2);

                    //int block_size = locations.size() * this.result_size;

                    for(Sketch currentSketch: locations) {

                        for(int location: currentSketch.getFeatures()) {

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);

                            if(locations_obtained != null) {

                                for (int i = 0; i< locations_obtained.length; ++i){

                                    if (current_results.containsKey(locations_obtained[i])) {
                                        current_results.put(locations_obtained[i], current_results.get(locations_obtained[i]) + 1);
                                    }
                                    else {

                                        current_results.put(locations_obtained[i], 1);
                                    }

                                }
                            }
                        }
                    }

                    for(Sketch currentSketch: locations2) {

                        for(int location: currentSketch.getFeatures()) {

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);

                            if(locations_obtained != null) {

                                for (int i = 0; i< locations_obtained.length; ++i){

                                    if (current_results.containsKey(locations_obtained[i])) {
                                        current_results.put(locations_obtained[i], current_results.get(locations_obtained[i]) + 1);
                                    }
                                    else {

                                        current_results.put(locations_obtained[i], 1);
                                    }

                                }
                            }
                        }
                    }

                    finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(currentSequence, current_results));

                    currentSequence++;

                    locations.clear();
                    locations2.clear();
                }

                seqReader.close();
                seqReader2.close();
            }

            return finalResults.iterator();

        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativeTreeMapPaired: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}