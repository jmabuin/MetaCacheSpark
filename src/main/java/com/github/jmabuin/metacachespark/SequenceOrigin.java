package com.github.jmabuin.metacachespark;

import java.io.Serializable;

/**
 * Created by chema on 1/19/17.
 */
public class SequenceOrigin implements Serializable {


	private String filename = "";
	private int index = 0;  //if file contains more than one sequence. This is the TGT



	public SequenceOrigin(String filename, int index) {
		this.filename = filename;
		this.index = index;
	}

	public SequenceOrigin() {

	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

}
