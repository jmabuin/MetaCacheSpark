package com.github.metacachespark;

import java.io.Serializable;

/**
 * Created by chema on 1/18/17.
 */
public class Location implements Serializable{

	private int key;
	private short tgtID;
	private int winID;
	private int partitionID;
	private String genomeFileName;
	private String genomeHeader;


	public Location() {
		this.tgtID = 0;
		this.winID = 0;
		this.key = 0;
		this.partitionID = 0;
		this.genomeFileName = "";
		this.genomeHeader = "";
	}

	public Location(int key, short tgt, int win, int partition, String name, String genomeHeader) {

		this.key = key;
		this.tgtID = tgt;
		this.winID = win;
		this.partitionID = partition;
		this.genomeFileName = name;
		this.genomeHeader = genomeHeader;
	}

	public static boolean lt (Location a, Location b) {
		if(a.tgtID < b.tgtID) return true;
		if(a.tgtID > b.tgtID) return false;
		return (a.winID < b.winID);
	}

	public boolean lt(Location other) {
		if(this.tgtID < other.tgtID) return true;
		if(this.tgtID > other.tgtID) return false;
		return (this.winID < other.winID);
	}

	public static boolean gt (Location a, Location b) {
		if(a.tgtID > b.tgtID) return true;
		if(a.tgtID < b.tgtID) return false;
		return (a.winID > b.winID);
	}

	public boolean gt(Location other) {
		if(this.tgtID > other.tgtID) return true;
		if(this.tgtID < other.tgtID) return false;
		return (this.winID > other.winID);
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public short getTgtID() {
		return tgtID;
	}

	public void setTgtID(short tgtID) {
		this.tgtID = tgtID;
	}

	public int getWinID() {
		return winID;
	}

	public void setWinID(int winID) {
		this.winID = winID;
	}

	public int getPartitionID() {
		return partitionID;
	}

	public void setPartitionID(int partitionID) {
		this.partitionID = partitionID;
	}

	public String getGenomeFileName() {
		return genomeFileName;
	}

	public void setGenomeFileName(String genomeFileName) {
		this.genomeFileName = genomeFileName;
	}

	public String getGenomeHeader() {
		return genomeHeader;
	}

	public void setGenomeHeader(String genomeHeader) {
		this.genomeHeader = genomeHeader;
	}
}
