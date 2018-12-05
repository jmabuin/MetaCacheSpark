package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.TargetProperty;
import com.github.jmabuin.metacachespark.database.*;
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
public class PartialQueryNativeListPaired implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, List<MatchCandidate>> {

    private static final Log LOG = LogFactory.getLog(PartialQueryNativeListPaired.class);

    private String fileName;
    private String fileName2;
    private long init;
    private int bufferSize;
    private List<TargetProperty> targets_;
    private Taxonomy taxa_;
    private MetaCacheOptions options;
    private long window_stride;

    public PartialQueryNativeListPaired(String file_name, String file_name2, long init, int bufferSize//) {
                                        ,long window_stride, MetaCacheOptions options, Taxonomy taxa_, List<TargetProperty> targets_) {
        this.fileName = file_name;
        this.fileName2 = file_name2;
        this.init = init;
        this.bufferSize = bufferSize;
        this.targets_ = targets_;
        this.taxa_ = taxa_;
        this.options = options;
        this.window_stride = window_stride;

    }

    public PartialQueryNativeListPaired(String file_name, String file_name2, long init, int bufferSize) {
        this.fileName = file_name;
        this.fileName2 = file_name2;
        this.init = init;
        this.bufferSize = bufferSize;
        //this.targets_ = targets_;
        //this.taxa_ = taxa_;


    }

    @Override
    public Iterator<Tuple2<Long, List<MatchCandidate>>> call(Iterator<HashMultiMapNative> myHashMaps) {

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

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);

                            if(locations_obtained != null) {


                                for (int i = 0; i< locations_obtained.length; ++i){

                                    current_results.add(locations_obtained[i]);

                                }
                            }
                        }
                    }

                    for(Sketch currentSketch: locations2) {

                        for(int location: currentSketch.getFeatures()) {

                            LocationBasic locations_obtained[] = currentHashMap.get_locations(location);

                            if(locations_obtained != null) {


                                for (int i = 0; i< locations_obtained.length; ++i){

                                    current_results.add(locations_obtained[i]);

                                }
                            }
                        }
                    }

                    // These hits are already the TOP hits
                    List<MatchCandidate> hits = this.insert_all(current_results, numWindows);

                    //LOG.warn("Items for sequence " + currentSequence +" is: " + hits.size());

                    //LOG.warn("Adding " + current_results.size() +" to sequence " + currentSequence);
                    finalResults.add(new Tuple2<Long, List<MatchCandidate>>(currentSequence, hits));

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
            LOG.error("ERROR in PartialQueryNativeListPaired: "+e.getMessage());
            System.exit(-1);
        }

        return finalResults.iterator();
    }


    private List<MatchCandidate> insert_all(List<LocationBasic> all_hits, long num_windows) {

        HashMap<Integer, MatchCandidate> hits_map = new HashMap<>();
        List<MatchCandidate> top_list = new ArrayList<>();

        CandidateGenerationRules rules = new CandidateGenerationRules();
        //rules.setMaxWindowsInRange((int)num_windows);

        //rules.setMaxWindowsInRange(numWindows);

        if(all_hits.isEmpty()) {
            //LOG.warn("Matches is empty!");
            return top_list;
        }
        //else {
        //    LOG.warn("We have matches!");
        //}

        //Sort candidates list in ASCENDING order by tgt and window
        Collections.sort(all_hits, new Comparator<LocationBasic>() {
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
        LocationBasic fst = all_hits.get(0);//matches.firstEntry();

        /*int loc = 0;
        for(LocationBasic l: all_hits) {
            if (loc <5) {
                LOG.warn("Tgt: " + l.getTargetId() + ", Win: " + l.getWindowId());
            }
            else {
                break;
            }
            loc++;
        }*/

        long hits = 1;
        MatchCandidate curBest = new MatchCandidate();
        curBest.setTax(this.get_taxon(fst));
        curBest.setTgt(fst.getTargetId());
        curBest.setHits(hits);
        curBest.setPos_beg(fst.getWindowId());
        curBest.setPos_end(fst.getWindowId());

        LocationBasic lst = fst;

        int entryFST = 0;
        int entryLST;

        // Iterate over candidates
        //LOG.warn("processing Candidate list");
        for(entryLST = entryFST + 1; entryLST< all_hits.size(); entryLST++) {

            lst = all_hits.get(entryLST);

            //LOG.warn("Candidate " + lst.getTargetId());
            //LOG.warn("Candidate window is: "+lst.getKey().getWindowId());

            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst.getTargetId() == curBest.getTgt()) {
                //add new hits to the right
                hits++;
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
                //this.insert(curBest);
                this.insert_into_hashmap(hits_map, new MatchCandidate(curBest.getTgt(), hits, curBest.getPos(), curBest.getTax()), rules.getMaxCandidates());
                //reset to new target
                entryFST = entryLST;
                fst = all_hits.get(entryFST);
                hits = 1;
                curBest.setTgt(fst.getTargetId());
                curBest.setTax(this.get_taxon(fst));
                curBest.setPos_beg(fst.getWindowId());
                curBest.setPos_end(fst.getWindowId());
                curBest.setHits(hits);
            }


        }

        this.insert_into_hashmap(hits_map, new MatchCandidate(curBest.getTgt(), curBest.getHits(), curBest.getPos(), curBest.getTax()),
                rules.getMaxCandidates());

        if (!hits_map.isEmpty()) {

            //LOG.warn("Size of hitsmap is: "+all_hits.size());
            List<Map.Entry<Integer, MatchCandidate>> list = new ArrayList<>(hits_map.entrySet());
            //list.sort(Map.Entry.comparingByValue());

            // Sorting the list in DESCENDING order based on hits
            Collections.sort(list, new Comparator<Map.Entry<Integer, MatchCandidate>>() {
                public int compare(Map.Entry<Integer, MatchCandidate> o1,
                                   Map.Entry<Integer, MatchCandidate> o2) {

                    if (o1.getValue().getHits() < o2.getValue().getHits()) {
                        return 1;
                    }

                    if (o1.getValue().getHits() > o2.getValue().getHits()) {
                        return -1;
                    }

                    return 0;

                }
            });

            //Collections.reverse(list);

            MatchCandidate best = list.get(0).getValue();
            //this.best.setHits(list.get(list.size()-1).getValue());
            //this.lca = this.best.getTax();

            if (best.getHits() < this.options.getProperties().getHitsMin()) {
                return new ArrayList<MatchCandidate>();
            }

            double threshold = best.getHits() > this.options.getProperties().getHitsMin() ?
                    (best.getHits() - this.options.getProperties().getHitsMin()) *
                            this.options.getProperties().getHitsDiffFraction() : 0;
            //LOG.warn("Threshold: Best hits: " + best.getHits());
            //LOG.warn("Threshold: " + threshold);
            for (int i = 0; i < list.size(); ++i) {
                Map.Entry<Integer, MatchCandidate> current_entry = list.get(i);

				/*if (i>list.size()-5){
					LOG.warn("Best candidate item : " + i + " :: " + current_entry.getValue().getTgt() + " :: " + current_entry.getValue().getHits());
				}*/
                if (current_entry.getValue().getHits() > threshold) {
                    MatchCandidate current_cand = current_entry.getValue();
                    //current_cand.setHits(current_entry.getValue());
                    top_list.add(current_cand);
                } else {
                    break;
                }

            }
        }
        else {
            LOG.warn("Hits list is empty!! ");
        }

        return top_list;

    }

    private void insert_into_hashmap(HashMap<Integer, MatchCandidate> hm, MatchCandidate cand, long max_candidates) {

        if (hm.containsKey(cand.getTgt())) {
            //LOG.warn("Contains key Processing in insert " + cand.getTgt() + " :: " + cand.getHits());
            if (cand.getHits() > hm.get(cand.getTgt()).getHits()) {
                hm.put(cand.getTgt(), cand);
            }
        }
        else if (hm.size() < max_candidates) {
            //LOG.warn("Inserting " + cand.getTgt() + " :: " + cand.getHits());
            hm.put(cand.getTgt(), cand);

        }

    }

    private Taxon get_taxon(LocationBasic entry) {
        //LOG.warn("Getting taxon for TgtId: " + entry.getTargetId());
        //LOG.warn("Target is: " + entry.getTargetId());
        //LOG.warn("Taxa from target in targets_ is: " + this.targets_.get(entry.getTargetId()).getTax());
        return this.taxa_.getTaxa_().get(this.targets_.get(entry.getTargetId()).getTax());
    }

}