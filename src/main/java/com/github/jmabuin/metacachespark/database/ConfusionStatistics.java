package com.github.jmabuin.metacachespark.database;

import java.io.Serializable;

/**
 * Created by chema on 2/9/17.
 */
public class ConfusionStatistics implements Serializable {

	private long tp_ = 0;
	private long fp_ = 0;
	private long tn_ = 0;
	private long fn_ = 0;

	public ConfusionStatistics() {
	}

	public ConfusionStatistics(ConfusionStatistics src) {
		this.tp_ = src.getTp_();
		this.fp_ = src.getFp_();
		this.tn_ = src.getTn_();
		this.fn_ = src.getFn_();
	}



	public void count_outcome_truth(boolean outcome, boolean truth) {
		if(outcome) {
			if(truth) {
				count_true_pos();
			}
			else {
				count_false_pos();
			}
		} else {
			if(truth) {
				count_true_neg();
			}
			else {
				count_false_neg();
			}
		}
	}

	public void count_true_pos(long times) { tp_ += times; }
	public void count_false_pos(long times){ fp_ += times; }
	public void count_true_neg(long times) { tn_ += times; }
	public void count_false_neg(long times){ fn_ += times; }

	public void count_true_pos() { tp_ += 1; }
	public void count_false_pos(){ fp_ += 1; }
	public void count_true_neg() { tn_ += 1; }
	public void count_false_neg(){ fn_ += 1; }

	public long true_pos() { return tp_; }
	public long false_pos() { return fp_; }
	public long true_neg() { return tn_; }
	public long false_neg() { return fn_; }

	public long condition_pos() { return true_pos() + false_neg(); }
	public long condition_neg() { return true_neg() + false_pos(); }
	public long outcome_pos() { return true_pos() + false_pos(); }
	public long outcome_neg() { return true_neg() + false_neg(); }

	public long total() {
		return condition_pos() + condition_neg();
	}

	public double outcome_pos_rate() {
		return outcome_pos() / (double)total();
	}

	double outcome_neg_rate() {
		return outcome_neg() / (double)total();
	}

	double accuracy() {
		return (true_pos() + true_neg()) / (double) total();
	}

	//true positive rate, hit rate, recall
	double sensitivity() {
		return true_pos() / (double)condition_pos();
	}
	//true negative rate
	double specificity() {
		return true_neg() / (double)condition_neg();
	}

	//positive predictive value
	double precision() {
		return true_pos() / (double)outcome_pos();
	}

	double negative_prediction() {
		return true_neg() / (double)outcome_neg();
	}

	double negative_omission() {
		return false_neg() / (double)outcome_neg();
	}

	double false_pos_rate() {
		return false_pos() / (double)condition_neg();
	}

	double false_discovery_rate() {
		return false_pos() / (double)outcome_pos();
	}

	double miss_rate() {
		return false_neg() / (double)condition_pos();
	}


	public long getTp_() {
		return tp_;
	}

	public void setTp_(long tp_) {
		this.tp_ = tp_;
	}

	public long getFp_() {
		return fp_;
	}

	public void setFp_(long fp_) {
		this.fp_ = fp_;
	}

	public long getTn_() {
		return tn_;
	}

	public void setTn_(long tn_) {
		this.tn_ = tn_;
	}

	public long getFn_() {
		return fn_;
	}

	public void setFn_(long fn_) {
		this.fn_ = fn_;
	}
}
