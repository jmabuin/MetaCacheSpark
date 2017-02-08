package com.github.jmabuin.metacachespark;
import java.io.*;

/**
 * Created by chema on 1/13/17.
 */
public class Taxon implements Serializable {



	private long taxonId;
	private long parentId;

	private String taxonName;
	private Taxonomy.Rank rank;

	public Taxon(long taxonId, long parentId, String taxonName, Taxonomy.Rank rank) {

		this.taxonId = taxonId;
		this.parentId = parentId;
		this.taxonName = taxonName;
		this.rank = rank;

	}

	public Taxon() {

		this.taxonId = 0;
		this.parentId = 0;
		this.taxonName = "--";
		this.rank = Taxonomy.Rank.none;


	}

	public Taxon(long taxonId) {

		this.taxonId = taxonId;
		this.parentId = 0;
		this.taxonName = "--";
		this.rank = Taxonomy.Rank.none;

	}

	/*@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Taxon taxon = (Taxon) o;

		return taxonId == taxon.taxonId;
	}*/
	@Override
	public boolean equals(Object o) {
		return this.taxonId == ((Taxon)o).taxonId;
	}

	@Override
	public int hashCode() {
		return (int) (taxonId ^ (taxonId >>> 32));
	}

	public static boolean lt(Taxon a, Taxon b){

		return a.taxonId < b.taxonId;
	}

	public boolean lt(Taxon other) {

		return this.taxonId < other.taxonId;

	}

	public static boolean gt(Taxon a, Taxon b) {

		return a.taxonId > b.taxonId;
	}

	public boolean gt (Taxon other) {
		return this.taxonId > other.taxonId;
	}

	public static boolean equals(Taxon a, Taxon b) {
		return a.taxonId == b.taxonId;

	}

	/*public boolean equals(Taxon other) {

		return this.taxonId == other.taxonId;

	}*/

	public boolean none() {
		return this.taxonId < 2;
	}

	public String rank_name() {

		return this.rank.name();

	}

	public void read_binary(ObjectInputStream istream, Taxon t){

		/*
		t.setTaxonId((Long)IOSerialize.read_binary(istream, IOSerialize.DataTypes.LONG));
		t.setParentId((Long)IOSerialize.read_binary(istream, IOSerialize.DataTypes.LONG));
		t.setRank((Taxonomy.Rank)IOSerialize.read_binary(istream, IOSerialize.DataTypes.RANK));
		t.setTaxonName((String)IOSerialize.read_binary(istream, IOSerialize.DataTypes.STRING));
		*/

		Taxon newTaxon = (Taxon) IOSerialize.read_binary(istream, IOSerialize.DataTypes.TAXON);

		t.setTaxonId(newTaxon.getTaxonId());
		t.setParentId(newTaxon.getParentId());
		t.setRank(newTaxon.rank);
		t.setTaxonName(newTaxon.getTaxonName());
	}

	public void write_binary(ObjectOutputStream ostream, Taxon t) {
		IOSerialize.write_binary(ostream, t, IOSerialize.DataTypes.TAXON);
	}

/*
	void read_binary(std::istream& is, taxon& t) {
		read_binary(is, t.id);
		read_binary(is, t.parent);
		read_binary(is, t.rank);
		read_binary(is, t.name);
	}

	//-----------------------------------------------------
	friend
	void write_binary(std::ostream& os, const taxon& t) {
		write_binary(os, t.id);
		write_binary(os, t.parent);
		write_binary(os, t.rank);
		write_binary(os, t.name);
	}
	*/

	public long getTaxonId() {
		return taxonId;
	}

	public void setTaxonId(long taxonId) {
		this.taxonId = taxonId;
	}

	public long getParentId() {
		return parentId;
	}

	public void setParentId(long parentId) {
		this.parentId = parentId;
	}

	public String getTaxonName() {
		return taxonName;
	}

	public void setTaxonName(String taxonName) {
		this.taxonName = taxonName;
	}

	public Taxonomy.Rank getRank() {
		return rank;
	}

	public void setRank(Taxonomy.Rank rank) {
		this.rank = rank;
	}
}

