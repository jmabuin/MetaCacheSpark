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

/**
 * Created by chema on 2/10/17.
 */
public class IndexRange {

	private long beg;
	private long end;

	public IndexRange() {
		this.beg = 0;
		this.end = 0;
	}

	public IndexRange(long beg, long end) {
		this.beg = beg;
		this.end = end;
	}

	public long getBeg() {
		return beg;
	}

	public void setBeg(long beg) {
		this.beg = beg;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public long size() {

		return this.end - this.beg;
	}

	public boolean empty() {
		return this.end == this.beg;
	}
}
