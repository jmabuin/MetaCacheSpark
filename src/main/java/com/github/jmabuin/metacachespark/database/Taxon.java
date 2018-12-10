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

import java.io.*;

/**
 * Class that represents a Taxon from a Taxonomy
 * @author Jose M. Abuin
 */
public class Taxon implements Serializable {

	private long taxonId;
	private long parentId;

	private String taxonName;
	private Taxonomy.Rank rank;

	/**
	 * Builder with arguments
	 * @param taxonId The Taxon ID
	 * @param parentId This Taxon parent ID
	 * @param taxonName This Taxon name
	 * @param rank this Taxon Rank
	 */
	public Taxon(long taxonId, long parentId, String taxonName, Taxonomy.Rank rank) {

		this.taxonId = taxonId;
		this.parentId = parentId;
		this.taxonName = taxonName;
		this.rank = rank;

	}

	/**
	 * Builder with no arguments
	 */
	public Taxon() {

		this.taxonId = 0;
		this.parentId = 0;
		this.taxonName = "--";
		this.rank = Taxonomy.Rank.none;


	}

	/**
	 * Builder given only the Taxon ID
	 * @param taxonId The Taxon ID
	 */
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

	/**
	 * Function to check if two Taxons are equals. We do it by checking its IDs
	 * @param o The other Taxon
	 * @return True if the Taxons are equals, false otherwise
	 */
	@Override
	public boolean equals(Object o) {
		return this.taxonId == ((Taxon)o).taxonId;
	}

	/**
	 * hashCode function
	 * @return A 32 bit integer representing this object hash
	 */
	@Override
	public int hashCode() {
		return (int) (taxonId ^ (taxonId >>> 32));
	}

	/**
	 * Function to check if a Taxon is less than another one
	 * @param a Taxon a
	 * @param b Taxon b
	 * @return True if a ID less than b ID
	 */
	public static boolean lt(Taxon a, Taxon b){

		return a.taxonId < b.taxonId;
	}

	/**
	 * Function to check if this Taxon is less than other
	 * @param other The other Taxon
	 * @return True if this Taxon ID is less than the other
	 */
	public boolean lt(Taxon other) {

		return this.taxonId < other.taxonId;

	}

	/**
	 * Function to check if a Taxon is greater than another one
	 * @param a Taxon a
	 * @param b Taxon b
	 * @return true if a ID is greater than b ID
	 */
	public static boolean gt(Taxon a, Taxon b) {

		return a.taxonId > b.taxonId;
	}

	/**
	 * Function to check if this Taxon is greater than another one
	 * @param other The other Taxon
	 * @return True if this Taxon is greater than the other. False otherwise
	 */
	public boolean gt (Taxon other) {
		return this.taxonId > other.taxonId;
	}

	/**
	 * Function to check if two Taxons are equals
	 * @param a Taxon a
	 * @param b Taxon b
	 * @return True if the two Taxon IDs are equals, False otherwise
	 */
	public static boolean equals(Taxon a, Taxon b) {
		return a.taxonId == b.taxonId;

	}

	/*public boolean equals(Taxon other) {

		return this.taxonId == other.taxonId;

	}*/

	/**
	 * Function to check if this Taxon is none
	 * @return true if ID is lese than 2. False otherwise
	 */
	public boolean none() {
		return this.taxonId < 2;
	}

	/**
	 *  Function that returns this Taxon Rank name
	 * @return A String containing the Rank name
	 */
	public String rank_name() {

		return this.rank.name();

	}

	/**
	 * Getter for the Taxon ID
	 * @return A long variable containing the Taxon ID
	 */
	public long getTaxonId() {
		return taxonId;
	}

	/**
	 * Setter for the Taxon ID
	 * @param taxonId The new taxon ID
	 */
	public void setTaxonId(long taxonId) {
		this.taxonId = taxonId;
	}

	/**
	 * Getter for the parent ID
	 * @return A long variable containing the Parent ID
	 */
	public long getParentId() {
		return parentId;
	}

	/**
	 * Setter for the parent ID
	 * @param parentId A long variable containing the new parent ID
	 */
	public void setParentId(long parentId) {
		this.parentId = parentId;
	}

	/**
	 * Getter for the Taxon name
	 * @return A String containing the Taxon name
	 */
	public String getTaxonName() {
		return taxonName;
	}

	/**
	 * Setter for the Taxon name
	 * @param taxonName A String containing the new Taxon name
	 */
	public void setTaxonName(String taxonName) {
		this.taxonName = taxonName;
	}

	/**
	 * Getter for the Rank in this Taxon
	 * @return The Rank from this Taxon
	 */
	public Taxonomy.Rank getRank() {
		return rank;
	}

	/**
	 * Setter for this Taxon
	 * @param rank The new Rank
	 */
	public void setRank(Taxonomy.Rank rank) {
		this.rank = rank;
	}
}

