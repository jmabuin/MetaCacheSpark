package com.github.jmabuin.metacachespark;
import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
//public class LocationBasic implements Serializable, Comparable<Location>{
public class LocationBasic implements Serializable {

	private int targetId;
	private int windowId;

	public LocationBasic(int targetId, int windowId) {
		//public Location(int targetId, int windowId) {

		this.targetId = targetId;
		this.windowId = windowId;

		//this.header = header;
		//this.taxid = taxid;
	}

	public LocationBasic() {


		this.targetId = -1;
		this.windowId = -1;
		//this.header = "";
		//this.taxid = -1;

	}


	public int getTargetId() {
		return this.targetId;
	}

	/*public void setTargetId(int targetId) {
		this.targetId = targetId;
	}
*/
	public int getWindowId() {
		return windowId;
	}
/*
	public void setWindowId(int windowId) {
		this.windowId = windowId;
	}
*/
	//@Override
	public boolean equals(Object other){
		if (other == null) return false;
		if (other == this) return true;
		if (!(other instanceof LocationBasic))return false;

		LocationBasic otherMyClass = (LocationBasic)other;

		if((otherMyClass.getTargetId() == this.getTargetId())
				&& (otherMyClass.getWindowId() == this.getWindowId())) {
			return true;
		}
		else {
			return false;
		}

	}

	//@Override
	public int hashCode() {

		return (this.targetId+"-"+this.windowId).hashCode();

	}




}
