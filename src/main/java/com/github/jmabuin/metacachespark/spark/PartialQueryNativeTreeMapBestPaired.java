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
import com.github.jmabuin.metacachespark.TargetProperty;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.io.*;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
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
public class PartialQueryNativeTreeMapBestPaired implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, List<LocationBasic>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNativeTreeMapBestPaired.class);

    private String fileName;
    private String fileName2;
    private long init;
    private int bufferSize;
    //private List<TargetProperty> targets_;
    //private Taxonomy taxa_;
    private MetaCacheOptions options;
    private long window_stride;

    public PartialQueryNativeTreeMapBestPaired(String file_name, String file_name2, long init, int bufferSize//) {
            , long window_stride, MetaCacheOptions options) {
        this.fileName = file_name;
        this.fileName2 = file_name2;
        this.init = init;
        this.bufferSize = bufferSize;
        //this.targets_ = targets_;
        //this.taxa_ = taxa_;
        this.options = options;
        this.window_stride = window_stride;

    }

    @Override
    public Iterator<Tuple2<Long, List<LocationBasic>>> call(Iterator<HashMultiMapNative> myHashMaps) {

        List<Tuple2<Long, List<LocationBasic>>> finalResults = new ArrayList<Tuple2<Long, List<LocationBasic>>>();



        try {
            SequenceFileReaderNative seqReader;
            SequenceFileReaderNative2 seqReader2;

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            Path hdfs_file_path = new Path(this.fileName);
            Path local_file_path = new Path(hdfs_file_path.getName());

            File tmp_file = new File(local_file_path.getName());

            if (!tmp_file.exists()) {
                fs.copyToLocalFile(hdfs_file_path, local_file_path);
                LOG.info("File " + local_file_path.getName() + " copied");
            } else {
                LOG.info("File " + local_file_path.getName() + " already exists. Not copying.");
            }

            Path hdfs_file_path2 = new Path(this.fileName2);
            Path local_file_path2 = new Path(hdfs_file_path2.getName());

            File tmp_file2 = new File(local_file_path2.getName());

            if (!tmp_file2.exists()) {
                fs.copyToLocalFile(hdfs_file_path2, local_file_path2);
                LOG.info("File " + local_file_path2.getName() + " copied");
            } else {
                LOG.info("File " + local_file_path2.getName() + " already exists. Not copying.");
            }

            String local_file_name = local_file_path.getName();
            String local_file_name2 = local_file_path2.getName();

            seqReader = new SequenceFileReaderNative(local_file_name);
            seqReader2 = new SequenceFileReaderNative2(local_file_name2);

            if (this.init != 0) {
                seqReader.skip(this.init);
                seqReader2.skip(this.init);
            }

            ArrayList<Sketch> locations = new ArrayList<Sketch>();
            ArrayList<Sketch> locations2 = new ArrayList<Sketch>();

            long currentSequence = this.init;

            // Theoretically there is only one HashMap per partition
            while (myHashMaps.hasNext()) {

                HashMultiMapNative currentHashMap = myHashMaps.next();

                //LOG.info("Processing hashmap " + currentSequence );

                LocationBasic loc = new LocationBasic();

                while ((seqReader.next() != null) && (seqReader2.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                    String header = seqReader.get_header();
                    String data = seqReader.get_data();
                    String qua = seqReader.get_quality();

                    String header2 = seqReader2.get_header();
                    String data2 = seqReader2.get_data();
                    String qua2 = seqReader2.get_quality();

                    long numWindows = (2 + Math.max(data.length() + data2.length(), this.options.getProperties().getInsertSizeMax()) / this.window_stride);

                    if (seqReader.get_header().isEmpty() || seqReader2.get_header().isEmpty()) {
                        continue;
                    }

                    // TreeMap where hits from this sequences will be stored
                    List<LocationBasic> current_results = new ArrayList<>();

                    if ((currentSequence == this.init) || (currentSequence == this.init + 1)) {
                        LOG.warn("Processing sequence " + currentSequence + " :: " + header + " :: " + header2);
                    }


                    SequenceData currentData = new SequenceData(header, data, qua);
                    SequenceData currentData2 = new SequenceData(header2, data2, qua2);

                    locations = SequenceFileReader.getSketchStatic(currentData);
                    locations2 = SequenceFileReader.getSketchStatic(currentData2);

                    //int block_size = locations.size() * this.result_size;

                    for (Sketch currentSketch : locations) {

                        for (int location : currentSketch.getFeatures()) {

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);

                            if (locations_obtained != null) {


                                for (int i = 0; i < locations_obtained.length; ++i) {

                                    current_results.add(locations_obtained[i]);

                                }
                            }
                        }
                    }

                    for (Sketch currentSketch : locations2) {

                        for (int location : currentSketch.getFeatures()) {

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);

                            if (locations_obtained != null) {


                                for (int i = 0; i < locations_obtained.length; ++i) {

                                    current_results.add(locations_obtained[i]);

                                }
                            }
                        }
                    }


                    //LOG.warn("Adding " + current_results.size() +" to sequence " + currentSequence);
                    finalResults.add(new Tuple2<Long, List<LocationBasic>>(currentSequence, current_results));

                    currentSequence++;

                    //current_results.clear();
                    locations.clear();
                    locations2.clear();
                }

                seqReader.close();
                seqReader2.close();
            }

            return finalResults.iterator();


        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativeTreeMapBestPaired: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}