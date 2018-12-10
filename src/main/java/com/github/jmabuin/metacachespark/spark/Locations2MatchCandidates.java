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
import com.github.jmabuin.metacachespark.TargetProperty;
import com.github.jmabuin.metacachespark.database.CandidateGenerationRules;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.database.Taxon;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class Locations2MatchCandidates implements PairFunction<Tuple2<Long, Iterable<List<LocationBasic>>>, Long, List<MatchCandidate>> {

    private static final Log LOG = LogFactory.getLog(Locations2MatchCandidates.class);

    private List<TargetProperty> targets_;
    private Taxonomy taxa_;
    private MetaCacheOptions options;
    private long window_stride;

    public Locations2MatchCandidates(long window_stride, MetaCacheOptions options, Taxonomy taxa_, List<TargetProperty> targets_) {
        this.targets_ = targets_;
        this.taxa_ = taxa_;
        this.options = options;
        this.window_stride = window_stride;
    }


    @Override
    public Tuple2<Long, List<MatchCandidate>> call(Tuple2<Long, Iterable<List<LocationBasic>>> input_locations) {

        long sequence_number = input_locations._1;

        List<LocationBasic> all_hits = new ArrayList<>();
        List<MatchCandidate> tophits = new ArrayList<>();

        for (List<LocationBasic> current_locations: input_locations._2) {

            all_hits.addAll(current_locations);

        }

        this.insert_all(all_hits, tophits, this.window_stride);


        return new Tuple2<Long, List<MatchCandidate>>(sequence_number, tophits);

    }

    private List<MatchCandidate> insert_all(List<LocationBasic> all_hits,List<MatchCandidate> tophits, long num_windows) {

        HashMap<Integer, MatchCandidate> hits_map = new HashMap<>();
        //List<MatchCandidate> top_list = new ArrayList<>();

        CandidateGenerationRules rules = new CandidateGenerationRules();
        rules.setMaxWindowsInRange((int)num_windows);

        if(all_hits.isEmpty()) {
            LOG.warn("Matches is empty!");
            return tophits;
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

        long hits = 1;
        MatchCandidate curBest = new MatchCandidate();
        curBest.setTax(this.get_taxon(fst));
        curBest.setTgt(fst.getTargetId());
        curBest.setHits(hits);
        curBest.setPos_beg(fst.getWindowId());
        curBest.setPos_end(fst.getWindowId());

        LocationBasic lst;// = fst;

        int entryFST = 0;
        int entryLST = entryFST + 1;

        // Iterate over candidates
        //LOG.warn("processing Candidate list");
        //for(entryLST = entryFST + 1; entryLST< all_hits.size(); entryLST++) {
        while(entryLST < all_hits.size()) {

            lst = all_hits.get(entryLST);

            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst.getTargetId() == curBest.getTgt()) {
                //add new hits to the right
                hits++;

                //subtract hits to the left that fall out of range
                while((entryFST != entryLST) && ((lst.getWindowId() - fst.getWindowId()) >= rules.getMaxWindowsInRange()))  {
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
                this.insert_into_hashmap(hits_map, new MatchCandidate(curBest.getTgt(), curBest.getHits(), curBest.getPos(), curBest.getTax()), rules.getMaxCandidates());
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

            ++entryLST;

        }

        this.insert_into_hashmap(hits_map, new MatchCandidate(curBest.getTgt(), curBest.getHits(), curBest.getPos(), curBest.getTax()),
                rules.getMaxCandidates());

        if (!hits_map.isEmpty()) {

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

            for (int i = 0; i < list.size(); ++i) {
                Map.Entry<Integer, MatchCandidate> current_entry = list.get(i);

                MatchCandidate current_cand = current_entry.getValue();
                //this.allhits.add(current_cand);

                if (current_entry.getValue().getHits() > threshold) {

                    //current_cand.setHits(current_entry.getValue());
                    tophits.add(current_cand);
                }

            }
        }
        else {
            LOG.warn("Hits list is empty!! ");
        }

        return tophits;

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

        return this.taxa_.getTaxa_().get(this.targets_.get(entry.getTargetId()).getTax());
    }

}
