package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Created by chema on 2/10/17.
 */

//matches_in_contiguous_window_range_top in original metacache

public class MatchesInWindowBasic {

	private static final Log LOG = LogFactory.getLog(MatchesInWindowBasic.class);

	private int numTgts_;
	private long hits_[];
	private long coveredWins_;
	private int tgt_[];
	private IndexRange pos_[];
	public static int maxNo = 2;
	private TreeMap<LocationBasic, Integer> matches;
	private TreeMap<LocationBasic, Integer> tophits;
	//private TreeMap<LocationBasic, Integer> matches_HM;

	public MatchesInWindowBasic(TreeMap<LocationBasic, Integer> matches, long numWindows) {
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
		Map.Entry<LocationBasic, Integer> fst = matches.firstEntry();
		Map.Entry<LocationBasic, Integer> lst = fst;

		ArrayList<Map.Entry<LocationBasic, Integer>> arrayListMatches = new ArrayList<Map.Entry<LocationBasic, Integer>>(matches.entrySet());

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
				//LOG.warn("tgt is: " + tgt + ", hits is: " + hits);
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

		//LOG.warn("Targets: " + tgt_[0] + " :: " + tgt_[1]);


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

	public TreeMap<LocationBasic, Integer> getMatches() {
		return matches;
	}
}
