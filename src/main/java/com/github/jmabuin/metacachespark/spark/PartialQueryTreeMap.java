package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.database.LocationBasicComparator;
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
public class PartialQueryTreeMap implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, TreeMap<LocationBasic, Integer>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryTreeMap.class);

    private String fileName;
    private long init;
    private int bufferSize;
    private long total;
    //private SequenceFileReader seqReader;
    private long readed;

    List<SequenceData> inputData;

    public PartialQueryTreeMap(List<SequenceData> inputData, long init, int bufferSize, long total, long readed) {
        //this.seqReader = seqReader;
        //this.fileName = fileName;
        this.init = init;
        this.bufferSize = bufferSize;
        this.total = total;
        this.readed = readed;
        this.inputData = inputData;
    }

    @Override
    public Iterator<Tuple2<Long, TreeMap<LocationBasic, Integer>>> call(Iterator<HashMultiMapNative> myHashMaps) {

        List<Tuple2<Long, TreeMap<LocationBasic, Integer>>> finalResults = new ArrayList<Tuple2<Long, TreeMap<LocationBasic, Integer>>>();

        try{



            ArrayList<Sketch> locations = new ArrayList<Sketch>();

            //HashMap<String, List<int[]>> results = new HashMap<String, List<int[]>>();

            //finalResults.add(results);

            long currentSequence = this.init;
            LOG.warn("[JMAbuin] Init at sequence: " + currentSequence);
            // Theoretically there is only one HashMap per partition

            //List<TreeMap<LocationBasic, Integer>> queryResults = new ArrayList<TreeMap<LocationBasic, Integer>>();

            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();
                //LOG.warn("[JMAbuin] New HashMultiMapNative: " + currentSequence);

                for(SequenceData currentData: inputData){


                    //if(currentSequence >= this.init) {

                    locations = SequenceFileReader.getSketchStatic(currentData);

                    if(locations == null) {
                        LOG.warn("[JMAbuin] Locations is null!!");
                    }

                    //TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());
                    TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>();

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
                    finalResults.add(new Tuple2<Long, TreeMap<LocationBasic, Integer>>(currentSequence, res));
                    //}


                    //data = seqReader.next();
                    currentSequence++;
                    locations.clear();
                }
                LOG.warn("[JMAbuin] Finished bunch of sequences");
            }

            //LOG.warn("[JMAbuin] Ending buffer " + currentSequence);

            //seqReader.close();

            return finalResults.iterator();


        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}