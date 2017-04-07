package com.github.jmabuin.metacachespark.database;

/**
 * Created by chema on 2/9/17.
 */
public class Classification {

	private long tid_;
	private Taxon tax_;

	public Classification(long tid_, Taxon tax_){

		this.tid_ = tid_;
		this.tax_ = tax_;
	}

	public Classification( Taxon tax) {
		this.tid_ = Database.invalid_target_id();
		this.tax_ = tax;
	}

	public Classification() {
		this.tid_ = Database.invalid_target_id();
		this.tax_ = null;
	}

	public boolean has_taxon() {
		return tax_ != null;
	}

	public boolean sequence_level() {
		return tid_ != Database.invalid_target_id();
	}

	public boolean none() {
		return !sequence_level() && !has_taxon();
	}

	public long target() {
		return tid_;
	}
    public Taxon tax() {
		return tax_;
	}

	public Taxonomy.Rank rank()  {
		return this.sequence_level() ? Taxonomy.Rank.Sequence : (this.has_taxon() ? tax_.getRank() : Taxonomy.Rank.none);
	}
}
