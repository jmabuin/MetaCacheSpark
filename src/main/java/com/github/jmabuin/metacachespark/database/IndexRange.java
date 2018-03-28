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
