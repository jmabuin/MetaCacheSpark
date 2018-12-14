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

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.database.CandidateGenerationRules;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.io.SequenceData;
import com.github.jmabuin.metacachespark.io.SequenceFileReader;
import com.github.jmabuin.metacachespark.io.SequenceFileReaderNative;
import com.github.jmabuin.metacachespark.io.SequenceFileReaderNative2;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * Created by Jose M. Abuin on 3/28/17.
 */
public class PartialQueryJavaPaired implements PairFlatMapFunction<Iterator<HashMap<Integer, List<LocationBasic>>>, Long, List<MatchCandidate>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryJavaPaired.class);

    private String fileName;
    private String fileName2;
    private long init;
    private int bufferSize;
    //private List<TargetProperty> targets_;
    //private Taxonomy taxa_;
    private MetaCacheOptions options;
    private long window_stride;

    public PartialQueryJavaPaired(String file_name, String file_name2, long init, int bufferSize,long window_stride, MetaCacheOptions options){
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
    public Iterator<Tuple2<Long, List<MatchCandidate>>> call(Iterator<HashMap<Integer, List<LocationBasic>>> myHashMaps) {
//long initTime = System.nanoTime();

        List<Tuple2<Long, List<MatchCandidate>>> finalResults = new ArrayList<Tuple2<Long, List<MatchCandidate>>>();

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

                HashMap<Integer, List<LocationBasic>> currentHashMap = myHashMaps.next();

                //LOG.info("Processing hashmap " + currentSequence );

                while((seqReader.next() != null) && (seqReader2.next() != null) && (currentSequence < (this.init + this.bufferSize))) {

                    String header = seqReader.get_header();
                    String data = seqReader.get_data();
                    String qua = seqReader.get_quality();

                    String header2 = seqReader2.get_header();
                    String data2 = seqReader2.get_data();
                    String qua2 = seqReader2.get_quality();

                    long numWindows = ( 2 + Math.max(data.length() + data2.length(), this.options.getProperties().getInsertSizeMax()) / this.window_stride);

                    if (seqReader.get_header().isEmpty() || seqReader2.get_header().isEmpty()) {
                        continue;
                    }

                    // TreeMap where hits from this sequences will be stored
                    List<LocationBasic> current_results = new ArrayList<>();

                    if ((currentSequence == this.init) || (currentSequence == this.init +1 )) {
                        LOG.warn("Processing sequence " + currentSequence + " :: " + header + " :: " + header2);
                    }


                    SequenceData currentData = new SequenceData(header, data, qua);
                    SequenceData currentData2 = new SequenceData(header2, data2, qua2);

                    locations = SequenceFileReader.getSketchStatic(currentData);
                    locations2 = SequenceFileReader.getSketchStatic(currentData2);

                    //int block_size = locations.size() * this.result_size;

                    for(Sketch currentSketch: locations) {

                        for(int location: currentSketch.getFeatures()) {

                            List<LocationBasic> locations_obtained = currentHashMap.get(location);

                            if(locations_obtained != null) {

                                current_results.addAll(locations_obtained);
                            }
                        }
                    }

                    for(Sketch currentSketch: locations2) {

                        for(int location: currentSketch.getFeatures()) {

                            List<LocationBasic> locations_obtained = currentHashMap.get(location);

                            if(locations_obtained != null) {

                                current_results.addAll(locations_obtained);
                            }
                        }
                    }

                    // These hits are already the TOP hits
                    List<MatchCandidate> hits = this.insert_all(current_results, numWindows);

                    finalResults.add(new Tuple2<Long, List<MatchCandidate>>(currentSequence, hits));

                    currentSequence++;

                    //current_results.clear();
                    locations.clear();
                    locations2.clear();
                }

                seqReader.close();
                seqReader2.close();
            }

            //long endTime = System.nanoTime();

            //LOG.warn("Time spent in executor is: " + ((endTime - initTime) / 1e9) + " seconds");

            return finalResults.iterator();

        }
        catch(Exception e) {
            LOG.error("ERROR in PartialQueryNativePaired: "+e.getMessage());
            System.exit(-1);
        }




        return finalResults.iterator();
    }

    private List<MatchCandidate> insert_all(List<LocationBasic> all_hits, long num_windows) {

        //HashMap<Integer, MatchCandidate> hits_map = new HashMap<>();
        List<MatchCandidate> best_hits = new ArrayList<>();
        List<MatchCandidate> top_list = new ArrayList<>();

        CandidateGenerationRules rules = new CandidateGenerationRules();
        rules.setMaxWindowsInRange((int)num_windows);

        //rules.setMaxWindowsInRange(numWindows);

        if(all_hits.isEmpty()) {
            //LOG.warn("Matches is empty!");
            return top_list;
        }
        //else {
        //    LOG.warn("We have matches!");
        //}

        //Sort candidates list in ASCENDING order by tgt and window
        //Collections.sort(all_hits, new Comparator<LocationBasic>() {
        all_hits.sort(new Comparator<LocationBasic>() {
            public int compare(LocationBasic o1,
                               LocationBasic o2)
            {

                if (o1.getTargetId() > o2.getTargetId()) {
                    return 1;
                }

                if (o1.getTargetId() < o2.getTargetId()) {
                    return -1;
                }

                if (o1.getTargetId() == o2.getTargetId()) {
                    if (o1.getWindowId() > o2.getWindowId()) {
                        return 1;
                    }

                    if (o1.getWindowId() < o2.getWindowId()) {
                        return -1;
                    }

                    return 0;

                }
                return 0;

            }
        });

        //check hits per query sequence
        LocationBasic fst = all_hits.get(0);
        LocationBasic lst;


        long hits = 1;
        MatchCandidate curBest = new MatchCandidate();
        //curBest.setTax(this.get_taxon(fst));
        curBest.setTgt(fst.getTargetId());
        curBest.setHits(hits);
        curBest.setPos_beg(fst.getWindowId());
        curBest.setPos_end(fst.getWindowId());

        int entryFST = 0;

        // Iterate over candidates
        for(int entryLST = entryFST +1; entryLST< all_hits.size(); entryLST++) {

            lst = all_hits.get(entryLST);

            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst.getTargetId() == curBest.getTgt()) {
                //add new hits to the right
                hits ++;
                //subtract hits to the left that fall out of range
                while(entryFST != entryLST && (lst.getWindowId() - fst.getWindowId()) >= rules.getMaxWindowsInRange())
                {
                    hits--;
                    //move left side of range
                    ++entryFST;
                    fst = all_hits.get(entryFST);
                    //win = fst.getKey().getWindowId();
                }
                //track best of the local sub-ranges
                if(hits > curBest.getHits()) {
                    curBest.setHits(hits);
                    curBest.setPos_beg(fst.getWindowId());
                    curBest.setPos_end(lst.getWindowId());
                }
            }
            else {
                //end of current target
                //if (curBest.getHits() > this.options.getProperties().getHitsMin()) {
                best_hits.add(new MatchCandidate(curBest.getTgt(), curBest.getHits(), curBest.getPos(), curBest.getTax()));
                //}
                //reset to new target
                entryFST = entryLST;
                //fst = all_hits.get_location(entryFST);
                fst = all_hits.get(entryFST);
                hits = 1;
                curBest.setTgt(fst.getTargetId());
                //curBest.setTax(this.get_taxon(fst));
                curBest.setPos_beg(fst.getWindowId());
                curBest.setPos_end(fst.getWindowId());
                curBest.setHits(hits);
            }


        }
        //if (curBest.getHits() > this.options.getProperties().getHitsMin()) {
        best_hits.add(new MatchCandidate(curBest.getTgt(), curBest.getHits(), curBest.getPos(), curBest.getTax()));
        //}

        if (best_hits.isEmpty()) {
            return new ArrayList<MatchCandidate>();
        }

        // Sorting the list in DESCENDING order based on hits
        best_hits.sort(new Comparator<MatchCandidate>() {
            public int compare(MatchCandidate o1,
                               MatchCandidate o2) {

                if (o1.getHits() < o2.getHits()) {
                    return 1;
                }

                if (o1.getHits() > o2.getHits()) {
                    return -1;
                }

                return 0;

            }
        });


        MatchCandidate best = best_hits.get(0);


        if (best.getHits() < this.options.getProperties().getHitsMin()) {
            return new ArrayList<MatchCandidate>();
        }

        double threshold = best.getHits() > this.options.getProperties().getHitsMin() ?
                (best.getHits() - this.options.getProperties().getHitsMin()) *
                        this.options.getProperties().getHitsDiffFraction() : 0;

        for (int i = 0; i < best_hits.size(); ++i) {
            MatchCandidate current_entry = best_hits.get(i);


            if (current_entry.getHits() > threshold) {
                top_list.add(current_entry);
            } else {
                break;
            }

        }



        return top_list;



    }


}