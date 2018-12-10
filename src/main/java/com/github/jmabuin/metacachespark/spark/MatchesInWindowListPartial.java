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
import com.github.jmabuin.metacachespark.TargetProperty;
import com.github.jmabuin.metacachespark.database.CandidateGenerationRules;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.database.Taxon;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.util.matching.Regex;

import java.io.Serializable;
import java.util.*;

/**
 * Created by chema on 2/10/17.
 */

//best_distinct_matches_in_contiguous_window_ranges in original metacache

public class MatchesInWindowListPartial implements Serializable {

    private static final Log LOG = LogFactory.getLog(MatchesInWindowListPartial.class);

    private int numTgts_;
    private long hits_[];
    public static int maxNo = 2;
    private List<LocationBasic> matches;
    private List<TargetProperty> targets_;
    private Taxonomy taxa_;
    private HashMap<Integer, MatchCandidate> all_hits;
    private List<MatchCandidate> top_list;
    private MatchCandidate best;
    private MetaCacheOptions options;
    private Taxon lca;

    private CandidateGenerationRules rules;
    //private TreeMap<LocationBasic, Integer> matches_HM;

    public MatchesInWindowListPartial(List<LocationBasic> matches, int numWindows, List<TargetProperty> targets_, Taxonomy taxa_, MetaCacheOptions options) {

        this.matches = matches;
        this.targets_ = targets_;
        this.taxa_ = taxa_;
        this.rules = new CandidateGenerationRules();

        this.rules.setMaxWindowsInRange(numWindows);

        this.options = options;

        this.all_hits = new HashMap<Integer, MatchCandidate>();
        this.top_list = new ArrayList<>();
        //ArrayList<Map.Entry<LocationBasic, Integer>> arrayListMatches = new ArrayList<Map.Entry<LocationBasic, Integer>>(matches.entrySet());

        if(matches.isEmpty()) {
            LOG.warn("Matches is empty!");
            return;
        }

        //Sort candidates list
        Collections.sort(matches, new Comparator<LocationBasic>() {
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

        //LOG.warn("Total locations in matches: " + matches.size());
		/*for (int i = 0; i< matches.size(); ++i) {

			LOG.warn("Target: " + matches.get(i).getTargetId() + " :: " + matches.get(i).getWindowId());

			if (i > 15) {
				break;
			}

		}*/

        //check hits per query sequence
        LocationBasic fst = this.matches.get(0);//matches.firstEntry();


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
        for(entryLST = entryFST + 1; entryLST< this.matches.size(); entryLST++) {

            lst = this.matches.get(entryLST);

            //LOG.warn("Candidate " + lst.getTargetId());
            //LOG.warn("Candidate window is: "+lst.getKey().getWindowId());

            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst.getTargetId() == curBest.getTgt()) {
                //add new hits to the right
                hits ++;
                //subtract hits to the left that fall out of range
                while(entryFST != entryLST &&	(lst.getWindowId() - fst.getWindowId()) >= this.rules.getMaxWindowsInRange())
                {
                    hits--;
                    //move left side of range
                    ++entryFST;
                    fst = this.matches.get(entryFST);
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
                this.insert(new MatchCandidate(curBest.getTgt(), hits, curBest.getPos(), curBest.getTax()));
                //reset to new target
                entryFST = entryLST;
                fst = this.matches.get(entryFST);
                hits = 1;
                curBest.setTgt(fst.getTargetId());
                curBest.setTax(this.get_taxon(fst));
                curBest.setPos_beg(fst.getWindowId());
                curBest.setPos_end(fst.getWindowId());
                curBest.setHits(hits);
            }


        }

        this.insert(new MatchCandidate(curBest.getTgt(), curBest.getHits(), curBest.getPos(), curBest.getTax()));

        if (!all_hits.isEmpty()) {

            //LOG.warn("Size of hitsmap is: "+all_hits.size());
            List<Map.Entry<Integer, MatchCandidate>> list = new ArrayList<>(all_hits.entrySet());
            //list.sort(Map.Entry.comparingByValue());

            // Sorting the list based on values
            Collections.sort(list, new Comparator<Map.Entry<Integer, MatchCandidate>>() {
                public int compare(Map.Entry<Integer, MatchCandidate> o1,
                                   Map.Entry<Integer, MatchCandidate> o2)
                {
					/*if (order)
					{
						return o1.getValue().compareTo(o2.getValue());
					}
					else
					{
						return o2.getValue().compareTo(o1.getValue());

					}*/
                    if (o1.getValue().getHits() > o2.getValue().getHits()) {
                        return 1;
                    }

                    if (o1.getValue().getHits() < o2.getValue().getHits()) {
                        return -1;
                    }

                    return 0;

                }
            });

            //LOG.warn("size of sorted hits list: " + list.size());
            //Collections.reverse(list);

            this.best = list.get(list.size()-1).getValue();
            //this.best.setHits(list.get(list.size()-1).getValue());
            this.lca = this.best.getTax();

            //LOG.warn("Best hits: " + this.best.getTgt() + " :: " + this.best.getHits());
            if (this.best.getHits() < this.options.getProperties().getHitsMin()) {
                return;
            }

            double threshold = this.best.getHits() > this.options.getProperties().getHitsMin() ?
                    (this.best.getHits() - this.options.getProperties().getHitsMin()) *
                            this.options.getProperties().getHitsDiffFraction() : 0;

            //LOG.warn("Threshold: " + threshold);
            for (int i = list.size() - 1;(i >= 0); --i) {
                Map.Entry<Integer, MatchCandidate> current_entry = list.get(i);

				/*if (i>list.size()-5){
					LOG.warn("item : " + current_entry.getValue().getTgt() + " :: " + current_entry.getValue().getHits());
				}*/
                if (current_entry.getValue().getHits() > threshold) {
                    MatchCandidate current_cand = current_entry.getValue();
                    //current_cand.setHits(current_entry.getValue());
                    this.top_list.add(current_cand);
                }
                else {
                    break;
                }

            }

			/*LOG.warn("Size of top list is: " + this.top_list.size());

			for(int i = 0; i< this.top_list.size(); i++) {
				LOG.warn("Item: " + top_list.get(i).getTgt() + " :: " + top_list.get(i).getHits());
				if (i > 5) {
					break;
				}
			}*/

        }

    }

    private Taxon get_taxon(LocationBasic entry) {
        //LOG.warn("Getting taxon for TgtId: " + entry.getTargetId());
        //LOG.warn("Target is: " + entry.getTargetId());
        //LOG.warn("Taxa from target in targets_ is: " + this.targets_.get(entry.getTargetId()).getTax());
        return this.taxa_.getTaxa_().get(this.targets_.get(entry.getTargetId()).getTax());
    }

    public static int max_count() {
        return maxNo;
    }

    public static long nvalid_tgt() {
        return Long.MAX_VALUE;
    }


    public int count() {
        for(int i = 0; i < maxNo; ++i) {
            if(hits_[i] < 1) return i;
        }
        return maxNo;
    }


    public long hits(int rank) {
        //return hits_[rank];
        return this.top_list.get(rank).getHits();
    }

    public int target_id(int rank) {
        return this.top_list.get(rank).getTgt();
    }

    public List<LocationBasic> getMatches() {
        return matches;
    }

    private boolean insert(MatchCandidate cand) {

		/*if (cand.getTax() == null) {
			LOG.warn("Tax is null");
			return false;
		}*/

        if (this.all_hits.containsKey(cand.getTgt())) {
            //LOG.warn("Contains key Processing in insert " + cand.getTgt() + " :: " + cand.getHits());
            if (cand.getHits() > this.all_hits.get(cand.getTgt()).getHits()) {
                this.all_hits.put(cand.getTgt(), cand);
            }
        }
        else if (this.all_hits.size() < this.rules.getMaxCandidates()) {
            //LOG.warn("Inserting " + cand.getTgt() + " :: " + cand.getHits());
            this.all_hits.put(cand.getTgt(), cand);

        }
        return true;

    }

    public HashMap<Integer, MatchCandidate> getAll_hits_() {
        return this.all_hits;
    }

    public MatchCandidate getBest() {
        return best;
    }

    public void setBest(MatchCandidate best) {
        this.best = best;
    }

    public Taxon getLca() {
        return lca;
    }

    public void setLca(Taxon lca) {
        this.lca = lca;
    }

    public int getWindows(){
        return this.rules.getMaxWindowsInRange();
    }

    public List<MatchCandidate> getTop_list(){
        return this.top_list;
    }
}
