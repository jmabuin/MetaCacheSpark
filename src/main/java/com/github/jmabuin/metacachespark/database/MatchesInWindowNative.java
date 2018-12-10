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

package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;

import java.util.*;

/**
 * Created by chema on 2/10/17.
 */

//matches_in_contiguous_window_range_top in original metacache

public class MatchesInWindowNative {

    private int numTgts_;
    private long hits_[];
    private long coveredWins_;
    private int tgt_[];
    private IndexRange pos_[];
    public static int maxNo = 2;
    private HashMap<LocationBasic, Integer> matches;
    //private TreeMap<LocationBasic, Integer> matches_HM;

    public MatchesInWindowNative(HashMap<LocationBasic, Integer> matches, long numWindows) {
        this.hits_ = new long[maxNo];
        this.tgt_ = new int[maxNo];
        this.pos_ = new IndexRange[maxNo];

        for(int i = 0; i< this.pos_.length; i++) {
            this.pos_[i] = new IndexRange();
        }

        this.coveredWins_ = numWindows;
        this.matches = matches;


        for(int i = 0; i < maxNo; ++i) {
            tgt_[i] = Integer.MAX_VALUE;
            hits_[i] = 0;
        }

        int tgt = Integer.MAX_VALUE;
        long hits = 0;
        long maxHits = 0;
        long win = 0;
        long maxWinBeg = 0;
        long maxWinEnd = 0;

        ArrayList<Map.Entry<LocationBasic, Integer>> arrayListMatches = new ArrayList<Map.Entry<LocationBasic, Integer>>(this.matches.entrySet());

        //check hits per query sequence
        Map.Entry<LocationBasic, Integer> fst = arrayListMatches.get(0);
        Map.Entry<LocationBasic, Integer> lst = fst;

        int entryFST = 0;

        for(int entryLST = 0; entryLST< arrayListMatches.size(); entryLST++) {
            lst = arrayListMatches.get(entryLST);

            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst.getKey().getTargetId() == tgt) {
                //add new hits to the right
                hits += lst.getValue();
                //subtract hits to the left that fall out of range
                while(fst != lst &&	(lst.getKey().getWindowId() - fst.getKey().getWindowId()) >= numWindows)
                {
                    hits -= fst.getValue();
                    //move left side of range
                    ++entryFST;
                    fst = arrayListMatches.get(entryFST);
                    win = fst.getKey().getWindowId();
                }
                //track best of the local sub-ranges
                if(hits > maxHits) {
                    maxHits = hits;
                    maxWinBeg = win;
                    maxWinEnd = win + Math.abs(entryLST - entryFST);//distance(fst,lst);
                }
            }
            else {
                //reset to new target
                ++numTgts_;
                win = arrayListMatches.get(entryLST).getKey().getWindowId();
                tgt = arrayListMatches.get(entryLST).getKey().getTargetId();
                hits = arrayListMatches.get(entryLST).getValue();
                maxHits = hits;
                maxWinBeg = win;
                maxWinEnd = win;
                //fst = lst;
                entryFST = entryLST;
            }
            //keep track of 'maxNo' largest
            //TODO binary search for large maxNo?
            for(int i = 0; i < maxNo; ++i) {
                if(maxHits >= hits_[i]) {
                    //shift to the right
                    for(int j = maxNo-1; j > i; --j) {
                        hits_[j] = hits_[j-1];
                        tgt_[j] = tgt_[j-1];
                        pos_[j] = pos_[j-1];
                    }
                    //set hits & associated sequence (position)
                    hits_[i] = maxHits;
                    tgt_[i] = tgt;
                    pos_[i].setBeg(maxWinBeg);
                    pos_[i].setEnd(maxWinEnd);
                    break;
                }
            }

        }


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

    public int target_id(int rank)  {
        return tgt_[rank];
    }

    public long hits(int rank) {
        return hits_[rank];
    }

    public long total_hits() {
        long h = 0;

        for(int i = 0; i < maxNo; ++i) {
            h += hits_[i];
        }

        return h;
    }

    public int target_ambiguity() {
        return numTgts_;
    }


    public IndexRange window(int rank) {
        return pos_[rank];
    }

    public long window_length(int rank) {
        return pos_[rank].getEnd() - pos_[rank].getBeg();
    }

    public long covered_windows() {
        return coveredWins_;
    }

    public HashMap<LocationBasic, Integer> getMatches() {
        return matches;
    }
}
