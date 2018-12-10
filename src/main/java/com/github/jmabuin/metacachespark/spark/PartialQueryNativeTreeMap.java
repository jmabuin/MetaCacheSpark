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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by Jose M. Abuin on 3/28/17.
 */
public class PartialQueryNativeTreeMap implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNativeTreeMap.class);

    private String fileName;
    private long init;
    private int bufferSize;
    private long total;
    private SequenceFileReaderNative seqReader;
    private long readed;
    private String local_file_name;
    private int result_size;

    public PartialQueryNativeTreeMap(String file_name, long init, int bufferSize, long total, long readed, int result_size) {
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

            long currentSequence = this.init;


            // Theoretically there is only one HashMap per partition
            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                //LOG.info("Processing hashmap " + currentSequence );

                LocationBasic loc = new LocationBasic();

                while((this.seqReader.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                    String header = this.seqReader.get_header();
                    String data = this.seqReader.get_data();
                    String qua = this.seqReader.get_quality();

                    if (this.seqReader.get_header().isEmpty()) {
                        continue;
                    }

                    // TreeMap where hits from this sequence will be stored
                    TreeMap<LocationBasic, Integer> current_results = new TreeMap<>();

                    if (currentSequence == this.init) {
                        LOG.warn("Processing sequence " + currentSequence + " :: " + header);
                    }


                    SequenceData currentData = new SequenceData(header, data, qua);

                    locations = SequenceFileReader.getSketchStatic(currentData);

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

                    finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(currentSequence, current_results));

                    currentSequence++;

                    locations.clear();
                }

                this.seqReader.close();
            }

            return finalResults.iterator();

        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativeTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}