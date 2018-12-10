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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by Jose M. Abuin on 3/28/17.
 */
public class PartialQueryJavaTreeMapSingle implements Function<HashMap<Integer, List<LocationBasic>>, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryJavaTreeMapSingle.class);

    private String fileName;
    private String local_file_name;
    private long current_read;
    private SequenceFileReaderNative seqReader;
    private int result_size;

    public PartialQueryJavaTreeMapSingle(String file_name, long current_read, int result_size) {
        this.fileName = file_name;
        this.current_read = current_read;
        this.result_size = result_size;
    }

    @Override
    public TreeMap<LocationBasic, Integer> call(HashMap<Integer, List<LocationBasic>> myHashMap) {

        TreeMap<LocationBasic, Integer> finalResults = new  TreeMap<LocationBasic, Integer>();

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

            if (this.current_read!=0) {
                this.seqReader.skip(this.current_read);
            }

            ArrayList<Sketch> locations = new ArrayList<Sketch>();

            List<SequenceData> inputData = new ArrayList<>();
            long currentSequence = this.current_read;

            //LOG.warn("Init at sequence: " + currentSequence);

            if (this.seqReader.next() != null) {
                //while((this.seqReader.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                //TreeMap<LocationBasic, Integer> current_results = new TreeMap<>();

                String header = this.seqReader.get_header();
                String data = this.seqReader.get_data();
                String qua = this.seqReader.get_quality();

                //LOG.info("Processing sequence " + currentSequence + " :: " + data);

                SequenceData currentData = new SequenceData(header, data, qua);

                locations = SequenceFileReader.getSketchStatic(currentData);

                if(locations == null) {
                    LOG.warn("Locations is null!!");
                }

                HashMap<LocationBasic, Integer> all_hits = new HashMap<>();

                //int block_size = locations.size() * this.result_size;

                for(Sketch currentSketch: locations) {

                    all_hits.clear();

                    for(int location: currentSketch.getFeatures()) {



                        List<LocationBasic> values = myHashMap.get(location);

                        if(values != null) {


                            for (LocationBasic loc: values) {

                                //LocationBasic loc = new LocationBasic(values[i], values[i + 1]);

                                if (all_hits.containsKey(loc)) {
                                    all_hits.put(loc, all_hits.get(loc) + 1);
                                }
                                else{
                                    all_hits.put(loc, 1);
                                }

                            }


                        }
                    }

                    List<Map.Entry<LocationBasic, Integer>> list = new ArrayList<>(all_hits.entrySet());
                    list.sort(Map.Entry.comparingByValue());
                    //Collections.reverse(list);

                    //for(Map.Entry<LocationBasic, Integer> current_entry : list) {
                    for( int i = list.size()-1, current_number_of_values = 0; (current_number_of_values < this.result_size) && (i>=0); --i, ++current_number_of_values){
                        Map.Entry<LocationBasic, Integer> current_entry = list.get(i);

                        if (finalResults.containsKey(current_entry.getKey())) {
                            finalResults.put(current_entry.getKey(), finalResults.get(current_entry.getKey()) + current_entry.getValue());
                        }
                        else {
                            finalResults.put(current_entry.getKey(), current_entry.getValue());
                        }

                    }


                }


                //finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(currentSequence, current_results));

                //data = seqReader.next();
                //currentSequence++;
                locations.clear();
            }

            this.seqReader.close();


            //LOG.warn("Ending buffer " + currentSequence);

            return finalResults;


        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativeTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults;
    }

}