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
public class FullQuery implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, List<int[]>> {

    private static final Log LOG = LogFactory.getLog(FullQuery.class);

    private String fileName;

    public FullQuery(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public Iterator<Tuple2<Long, List<int[]>>> call(Iterator<HashMultiMapNative> myHashMaps) {

        List<Tuple2<Long, List<int[]>>> finalResults = new ArrayList<Tuple2<Long, List<int[]>>>();

        try{

            ArrayList<Sketch> locations = new ArrayList<Sketch>();
            TreeMap<LocationBasic, Integer> matches;

            long totalReads = FilesysUtility.readsInFastaFile(this.fileName);
            long startRead = 0;


            // Theoretically there is only one HashMap per partition
            //long currentSequence = 0;
            LOG.info("Init at sequence: " + startRead);

            while(myHashMaps.hasNext()){

                HashMultiMapNative currentHashMap = myHashMaps.next();

                SequenceFileReader seqReader = new SequenceFileReader(this.fileName, 0);

                SequenceData currentData;

                for(startRead = 0; startRead < totalReads; startRead++) {

                    currentData = seqReader.next();
                    LOG.info("Processing sequence " + startRead);


                    //if(currentSequence >= this.init) {

                    locations = SequenceFileReader.getSketchStatic(currentData);

                    List<int[]> queryResults = new ArrayList<int[]>();

                    for(Sketch currentSketch: locations) {

                        for(int location: currentSketch.getFeatures()) {

                            int[] values = currentHashMap.get(location);

                            if(values != null) {

                                queryResults.add(values);

                            }
                        }

                    }

                    //results.put(data.getHeader(), queryResults);
                    finalResults.add(new Tuple2<Long, List<int[]>>(startRead, queryResults));
                    //}

                }
                seqReader.close();
                LOG.warn("[JMAbuin] Finished all sequences");
            }

            //LOG.warn("[JMAbuin] Ending buffer " + currentSequence);

            return finalResults.iterator();

        }
        catch(Exception e) {
            LOG.error("ERROR in FullQueryTreeMap: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }

}