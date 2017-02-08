package com.github.jmabuin.metacachespark;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by chema on 1/19/17.
 */
public class TargetProperty implements Serializable {

	private String identifier;
	private long tax;
	private SequenceOrigin origin;
	private ArrayList<Long> full_lineage;
	private Long ranked_lineage[];


	public TargetProperty (String identifier, long tax, SequenceOrigin origin) {
		this.identifier = identifier;
		this.tax = tax;
		this.origin = origin;
	}

	public TargetProperty(String identifier, long tax, SequenceOrigin origin, ArrayList<Long> full_lineage, Long[] ranked_lineage) {
		this.identifier = identifier;
		this.tax = tax;
		this.origin = origin;
		this.full_lineage = full_lineage;
		this.ranked_lineage = ranked_lineage;
	}

	public TargetProperty() {

	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public long getTax() {
		return tax;
	}

	public void setTax(long tax) {
		this.tax = tax;
	}

	public SequenceOrigin getOrigin() {
		return origin;
	}

	public void setOrigin(SequenceOrigin origin) {
		this.origin = origin;
	}

	public ArrayList<Long> getFull_lineage() {
		return full_lineage;
	}

	public void setFull_lineage(ArrayList<Long> full_lineage) {
		this.full_lineage = full_lineage;
	}

	public Long[] getRanked_lineage() {
		return ranked_lineage;
	}

	public void setRanked_lineage(Long[] ranked_lineage) {
		this.ranked_lineage = ranked_lineage;
	}

	/*
		friend void
	read_binary(std::istream& is, target_property& p) {
		read_binary(is, p.id);
		read_binary(is, p.taxonId);
		read_binary(is, p.ranks);
		read_binary(is, p.lineage);
		read_binary(is, p.origin.filename);
		read_binary(is, p.origin.index);
	}

		friend void
	write_binary(std::ostream& os, const target_property& p) {
		write_binary(os, p.id);
		write_binary(os, p.taxonId);
		write_binary(os, p.ranks);
		write_binary(os, p.lineage);
		write_binary(os, p.origin.filename);
		write_binary(os, p.origin.index);
	}

		std::string id;
		taxon_id taxonId;
		ranked_lineage ranks;
		full_lineage lineage;
		sequence_origin origin;
	};*/

}
