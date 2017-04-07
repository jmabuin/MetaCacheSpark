/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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
		this.full_lineage = new ArrayList<Long>();
		this.ranked_lineage = new Long[0];
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



}
