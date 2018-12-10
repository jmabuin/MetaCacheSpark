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

import com.github.jmabuin.metacachespark.FilesysUtility;
import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.io.SequenceData;
import com.github.jmabuin.metacachespark.io.SequenceFileReader;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by chema on 3/28/17.
 */
public class FullQueryTreeMap implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(FullQueryTreeMap.class);

    private String fileName;

    public FullQueryTreeMap(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public Iterator<Tuple2<Long, TreeMap<LocationBasic, Integer>>> call(Iterator<HashMultiMapNative> myHashMaps) {

        List<Tuple2<Long, TreeMap<LocationBasic, Integer>>> finalResults = new ArrayList<Tuple2<Long, TreeMap<LocationBasic, Integer>>>();

        try{

            ArrayList<Sketch> locations = new ArrayList<Sketch>();
            TreeMap<LocationBasic, Integer> matches;

            long totalReads = FilesysUtility.readsInFastaFile(this.fileName);
            long startRead = 0;


            // Theoretically there is only one HashMap per partition
            long currentSequence = 0;
            LOG.info("Init at sequence: " + currentSequence);

            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();
                LOG.info("New hasmap");



                SequenceFileReader seqReader = new SequenceFileReader(this.fileName, 0);

                SequenceData currentData;

                for(startRead = 0; startRead < totalReads; startRead++) {

                    currentData = seqReader.next();
                    LOG.info("Processing sequence " + startRead);
                    //if(currentSequence >= this.init) {

                    //TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());
                    TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>();

                    locations = SequenceFileReader.getSketchStatic(currentData);

                    if(locations == null) {
                        LOG.warn("[JMAbuin] Locations is null!!");
                    }


                    for(Sketch currentSketch: locations) {

                        if(currentSketch == null) {
                            LOG.warn("[JMAbuin] Sketch is null!!");
                        }

                        for(int location: currentSketch.getFeatures()) {

                            int[] values = currentHashMap.get(location);

                            if(values != null) {


                                for (int i = 0; i < values.length; i += 2) {

                                    LocationBasic loc = new LocationBasic(values[i], values[i + 1]);

                                    if (res.containsKey(loc)) {
                                        res.put(loc, res.get(loc) + 1);
                                    } else {
                                        res.put(loc, 1);
                                    }

                                }
                            }
                        }

                    }

                    //queryResults.add(res);

                    //results.put(data.getHeader(), queryResults);
                    finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(startRead, res));
                    //}


                    locations.clear();
                }
                seqReader.close();
                LOG.warn("[JMAbuin] Finished all sequences");
            }

            return finalResults.iterator();


        }
        catch(Exception e) {
            LOG.error("ERROR in FullQueryTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}