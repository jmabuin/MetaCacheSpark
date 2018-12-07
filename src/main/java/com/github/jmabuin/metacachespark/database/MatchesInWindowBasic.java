package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.TargetProperty;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
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
	private List<LocationBasic> matches;
	private List<MatchCandidate> tophits;
	private List<MatchCandidate> allhits;
	private MetaCacheOptions options;
	private Taxonomy taxa_;
	private List<TargetProperty> targets_;
	//private TreeMap<LocationBasic, Integer> matches_HM;

	public MatchesInWindowBasic(List<LocationBasic> matches, long numWindows, MetaCacheOptions options, Taxonomy taxa_, List<TargetProperty> targets_) {

		this.matches = matches;
		this.coveredWins_ = numWindows;
		this.options = options;
		this.taxa_ = taxa_;
		this.targets_ = targets_;

		this.tophits = new ArrayList<>();
		this.allhits = new ArrayList<>();

		this.insert_all(this.matches, this.coveredWins_);





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

	public List<LocationBasic> getMatches() {
		return matches;
	}

	public List<MatchCandidate> get_top_hits() {return this.tophits;}

	public List<MatchCandidate> get_all_hits() {return this.allhits;}


	private List<MatchCandidate> insert_all(List<LocationBasic> all_hits, long num_windows) {

		HashMap<Integer, MatchCandidate> hits_map = new HashMap<>();
		//List<MatchCandidate> top_list = new ArrayList<>();

		CandidateGenerationRules rules = new CandidateGenerationRules();
		rules.setMaxWindowsInRange((int)num_windows);

		//rules.setMaxWindowsInRange(numWindows);

		if(all_hits.isEmpty()) {
			LOG.warn("Matches is empty!");
			return this.tophits;
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
			//LOG.warn("Threshold: Best hits: " + best.getHits());
			//LOG.warn("Threshold: " + threshold);
			for (int i = 0; i < list.size(); ++i) {
				Map.Entry<Integer, MatchCandidate> current_entry = list.get(i);
                //LOG.warn("Best candidate item : " + i + " :: " + current_entry.getValue().getTgt() + " :: " + current_entry.getValue().getHits());
				/*if (i>list.size()-5){
					LOG.warn("Best candidate item : " + i + " :: " + current_entry.getValue().getTgt() + " :: " + current_entry.getValue().getHits());
				}*/
				MatchCandidate current_cand = current_entry.getValue();
				this.allhits.add(current_cand);

				if (current_entry.getValue().getHits() > threshold) {

					//current_cand.setHits(current_entry.getValue());
					this.tophits.add(current_cand);
				}

			}
		}
		else {
			LOG.warn("Hits list is empty!! ");
		}

		return this.tophits;

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
