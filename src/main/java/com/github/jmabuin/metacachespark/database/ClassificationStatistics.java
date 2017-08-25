package com.github.jmabuin.metacachespark.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by chema on 2/9/17.
 */
public class ClassificationStatistics {

	private static final Log LOG = LogFactory.getLog(ClassificationStatistics.class); // LOG to show messages

	private int assigned_[] = new int[Taxonomy.num_ranks + 1];
	private int known_[] = new int[Taxonomy.num_ranks + 1];
	private int correct_[] = new int[Taxonomy.num_ranks + 1];
	private int wrong_[] = new int[Taxonomy.num_ranks + 1];
	//private int alignmentScore_[] = new int[Taxonomy.num_ranks + 1];

	private ConfusionStatistics coverage_[] = new ConfusionStatistics[Taxonomy.num_ranks + 1];

	private AlignmentStatistics alignmentScore_;

	public ClassificationStatistics() {

		for(int x = 0; x < assigned_.length; x++) {
			assigned_[x] = 0;
		}

		for(int x = 0; x < known_.length; x++) {
			known_[x] = 0;
		}

		for(int x = 0; x < correct_.length; x++) {
			correct_[x] = 0;
		}

		for(int x = 0; x <  wrong_.length; x++) {
			wrong_[x] = 0;
		}

		for(int x = 0; x < this.coverage_.length; x++) {
			this.coverage_[x] = new ConfusionStatistics();
		}

		this.alignmentScore_ = new AlignmentStatistics();


	}

	/**
	 * @brief    counts one assignment
	 * @details  concurrency-safe
	 */
	public void assign(Taxonomy.Rank assigned) {
		if(assigned == Taxonomy.Rank.none) {
			++assigned_[Taxonomy.Rank.none.ordinal()];
		}
		else {
			for(int r = assigned.ordinal(); r <= Taxonomy.Rank.root.ordinal(); r++) {
				++assigned_[r];
			}
		}
	}


	/**
	 * @brief   counts one assignment including ground truth and correctness
	 *          assessment
	 *
	 * @details concurrency-safe
	 *
	 * @param assigned : lowest rank of current assignment
	 * @param known    : lowest rank for which ground truth was known
	 * @param correct  : lowest rank for which current assignment is correct
	 */
	public void assign_known_correct(Taxonomy.Rank assigned, Taxonomy.Rank known, Taxonomy.Rank correct) {

		assign(assigned);

		//plausibility check
		if(correct.ordinal() < assigned.ordinal()) correct = assigned;
		if(correct.ordinal() < known.ordinal())    correct = known;

		//if ground truth known -> count correct and wrong assignments
		if(known == Taxonomy.Rank.none) {
			//LOG.warn("[JMAbuin] Known is Rank.none!!");
			++known_[Taxonomy.Rank.none.ordinal()];
		}
		else {
			for(int r = known.ordinal(); r <= Taxonomy.Rank.root.ordinal(); ++r) ++known_[r];

			if(correct == Taxonomy.Rank.none) {
				++correct_[Taxonomy.Rank.none.ordinal()];
			}
			else {
				for(int r = correct.ordinal(); r <= Taxonomy.Rank.root.ordinal(); ++r) ++correct_[r];
			}

			//if ranks above and including the current rank of assignment
			//are wrong => levels below of current assignment must be wrong, too
			if(correct.ordinal() > assigned.ordinal()) {
				for(int r = known.ordinal(); r < correct.ordinal(); ++r) {
					++wrong_[r];
				}
			}
		}
	}

	/*

	void register_alignment_score(double score)
	{
		std::lock_guard<std::mutex> lock(mtx_);
		alignmentScore_ += score;
	}

    const alignment_statistics&
	alignment_scores() const noexcept {
		return alignmentScore_;
	}

	 */


	public void count_coverage_true_pos(Taxonomy.Rank r) {
		coverage_[r.ordinal()].count_true_pos();
	}

	public void count_coverage_false_pos(Taxonomy.Rank r) {
		coverage_[r.ordinal()].count_false_pos();
	}

	public void count_coverage_true_neg(Taxonomy.Rank r) {
		coverage_[r.ordinal()].count_true_neg();
	}

	public void count_coverage_false_neg(Taxonomy.Rank r) {
		coverage_[r.ordinal()].count_false_neg();
	}


	public ConfusionStatistics coverage(Taxonomy.Rank r) {
		LOG.warn("[JMAbuin] coverage length:: " + this.coverage_.length);
		LOG.warn("[JMAbuin] current rank:: " + r.ordinal());
		return coverage_[r.ordinal()];
	}

	public int assigned() {
		return assigned_[Taxonomy.Rank.root.ordinal()];
	}

	/**
	 * @brief number of assignments on a taxonomix rank (and above)
	 */
	public int assigned(Taxonomy.Rank r) {
		return assigned_[r.ordinal()];
	}

	public int unassigned() {
		return assigned_[Taxonomy.Rank.none.ordinal()];
	}

	public int total() {
		return assigned() + unassigned();
	}

	public int known() {
		return known_[Taxonomy.Rank.root.ordinal()];
	}

	/**
	 * @brief number of cases with ground truth known on ranks >= r
	 */
	public int known(Taxonomy.Rank r) {
		return known_[r.ordinal()];
	}

	public int unknown() {
		return known_[Taxonomy.Rank.none.ordinal()];
	}

	/**
	 * @brief number of known correct assignments on ranks >= r
	 */
	public int correct(Taxonomy.Rank r) {
		return correct_[r.ordinal()];
	}

	public int correct() {
		return correct_[Taxonomy.Rank.root.ordinal()];
	}

	/**
	 * @brief number of known wrong assignments on ranks <= r
	 */
	public int wrong(Taxonomy.Rank r) {
		return wrong_[r.ordinal()];
	}

	public int wrong() {
		return wrong_[Taxonomy.Rank.root.ordinal()];
	}


	//---------------------------------------------------------------
	public double known_rate(Taxonomy.Rank r) {
		return total() > 0 ?  known(r) / (double)total() : 0;
	}
	public double known_rate() {
		return total() > 0 ?  known() / (double)total() : 0;
	}
	public double unknown_rate() {
		return total() > 0 ?  unknown() / (double)total() : 0;
	}
	public double classification_rate(Taxonomy.Rank r) {
		return total() > 0 ? assigned(r) / (double)total() : 0;
	}
	public double unclassified_rate() {
		return total() > 0 ? unassigned() / (double)total() : 0;
	}

	public double sensitivity(Taxonomy.Rank r) {
		return known(r) > 0 ? correct(r) / (double)known(r) : 0;
	}

	public double precision(Taxonomy.Rank r) {
		//note that in general tot != assigned(r) and tot != known(r)
		double tot = correct(r) + wrong(r);
		return tot > 0 ? correct(r) / tot : 0;
	}

}
