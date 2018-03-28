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
    private List<int[]> matches;
    //private TreeMap<LocationBasic, Integer> matches_HM;

    public MatchesInWindowNative(List<int[]> matches, long numWindows) {
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

        //check hits per query sequence
        //TODO: Iterate over native map
        /*Map.Entry<LocationBasic, Integer> fst = matches.firstEntry();
        Map.Entry<LocationBasic, Integer> lst = fst;

        ArrayList<Map.Entry<LocationBasic, Integer>> arrayListMatches = new ArrayList<Map.Entry<LocationBasic, Integer>>(matches.entrySet());
        */

        int fst[] = matches.get(0);
        int lst[] = matches.get(0);

        int entryFST = 0;

        for(int entryLST = 0; entryLST< matches.size(); entryLST++) {
            lst = matches.get(entryLST);

            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst[0] == tgt) {
                //add new hits to the right
                hits += lst[2];
                //subtract hits to the left that fall out of range
                while(fst != lst &&	(lst[1] - fst[1]) >= numWindows)
                {
                    hits -= fst[2];
                    //move left side of range
                    ++entryFST;

                    fst = matches.get(entryFST);
                    win = fst[1];
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
                /*
                win = arrayListMatches.get(entryLST).getKey().getWindowId();
                tgt = arrayListMatches.get(entryLST).getKey().getTargetId();
                hits = arrayListMatches.get(entryLST).getValue();
                */
                win = lst[1];
                tgt = lst[0];
                hits = lst[2];


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


		/* Original code
		//check hits per query sequence
        auto fst = begin(matches);
        auto lst = fst;
        while(lst != end(matches)) {
            //look for neighboring windows with the highest total hit count
            //as long as we are in the same target and the windows are in a
            //contiguous range
            if(lst->first.tgt == tgt) {
                //add new hits to the right
                hits += lst->second;
                //subtract hits to the left that fall out of range
                while(fst != lst &&
                     (lst->first.win - fst->first.win) >= numWindows)
                {
                    hits -= fst->second;
                    //move left side of range
                    ++fst;
                    win = fst->first.win;
                }
                //track best of the local sub-ranges
                if(hits > maxHits) {
                    maxHits = hits;
                    maxWinBeg = win;
                    maxWinEnd = win + distance(fst,lst);
                }
            }
            else {
                //reset to new target
                ++numTgts_;
                win = lst->first.win;
                tgt = lst->first.tgt;
                hits = lst->second;
                maxHits = hits;
                maxWinBeg = win;
                maxWinEnd = win;
                fst = lst;
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
                    pos_[i].beg = maxWinBeg;
                    pos_[i].end = maxWinEnd;
                    break;
                }
            }
            ++lst;
        }
		 */


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

    public List<int[]> getMatches() {
        return matches;
    }
}
