package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

public class Sketcher2PairPartitionsArray implements FlatMapFunction<Iterator<Sequence>, int[]> {

    private static final Log LOG = LogFactory.getLog(Sketcher2PairPartitionsArray.class);

    private TreeMap<String, Long> sequencesIndexes;
    private int firstEmptyPosition;
    private MetaCacheOptions options;

    public Sketcher2PairPartitionsArray(TreeMap<String, Long> sequencesIndexes, MetaCacheOptions options) {

        this.sequencesIndexes = sequencesIndexes;
        this.options = options;
        //this.firstEmptyPosition = 0;
        //this.lookup = new HashMap<Integer, Integer>();
    }

    @Override
    public Iterator<int[]> call(Iterator<Sequence> sequenceIterator) throws Exception {
        ArrayList<int[]> returnedValues = new ArrayList<int[]>();
        //HashMultiMapNative map = new HashMultiMapNative(254);

        int currentStart;
        int currentEnd;

        //String currentWindow;
        int numWindows;
        int current_sketch_size;

        //ArrayList<Tuple2<Integer, LocationBasic>> returnedValues_local = new ArrayList<Tuple2<Integer, LocationBasic>>();

        while(sequenceIterator.hasNext()) {
            Sequence inputSequence = sequenceIterator.next();


            currentStart = 0;
            currentEnd = MCSConfiguration.windowSize;

            //String currentWindow = "";
            //StringBuffer currentWindow = new StringBuffer();
            numWindows = 0;

            //LOG.warn("Processing sequence: " + inputSequence.getHeader());
            // We iterate over windows (with overlap)

            while (currentStart < (inputSequence.getData().length() - MCSConfiguration.kmerSize)) {
                //Sketch resultSketch = new Sketch();

                if (currentEnd > inputSequence.getData().length()) {
                    currentEnd = inputSequence.getData().length();
                }

                current_sketch_size = MCSConfiguration.sketchSize;

                if ((currentEnd - currentStart) >= MCSConfiguration.kmerSize) {

                    if (currentEnd - currentStart < MCSConfiguration.kmerSize * 2){
                        current_sketch_size = currentEnd - currentStart - MCSConfiguration.kmerSize + 1;
                    }

                    // We compute the k-mers. In C
                    int sketchValues[] = HashFunctions.window2sketch32(inputSequence.getData().substring(currentStart, currentEnd)
                            , current_sketch_size, MCSConfiguration.kmerSize);

                    if (sketchValues != null) {
                        //LOG.warn("[JMAbuin] CurrentWindow sketch size: " + sketchValues.length);

                        for (int newValue : sketchValues) {

                            int[] new_array = new int[3];

                            new_array[0] = newValue;  //key
                            new_array[1] = this.sequencesIndexes.get(inputSequence.getSeqId()).intValue(); //tgt
                            new_array[2] = numWindows;

                            returnedValues.add(new_array);


                        }
                    }


                }
                numWindows++;
                currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
                currentEnd = currentStart + MCSConfiguration.windowSize;

            }

/*
			for(Tuple2<Integer, LocationBasic> current_loc: returnedValues_local) {

				map.add(current_loc._1(), current_loc._2().getTargetId(), current_loc._2().getWindowId());

			}

			returnedValues_local.clear();
*/

        }




        return returnedValues.iterator();
    }
}
