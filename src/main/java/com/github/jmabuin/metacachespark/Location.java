package com.github.jmabuin.metacachespark;
import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Location implements Serializable{
//public class Location implements Serializable {

    private int key;
    private int targetId;
    private int windowId;

    public Location(int key, int targetId, int windowId) {
	//public Location(int targetId, int windowId) {
        this.key = key;
        this.targetId = targetId;
        this.windowId = windowId;

        //this.header = header;
        //this.taxid = taxid;
    }

    public Location() {

        this.key = Integer.MAX_VALUE;
        this.targetId = -1;
        this.windowId = -1;
        //this.header = "";
        //this.taxid = -1;

    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

	public int getTargetId() {
		return this.targetId;
	}

	public void setTargetId(int targetId) {
    	this.targetId = targetId;
	}

	public int getWindowId() {
		return windowId;
	}

	public void setWindowId(int windowId) {
		this.windowId = windowId;
	}

	@Override
	public boolean equals(Object other){
		if (other == null) return false;
		if (other == this) return true;
		if (!(other instanceof Location))return false;
		Location otherMyClass = (Location)other;

		if((otherMyClass.getKey() == this.getKey())
				&& (otherMyClass.getTargetId() == this.getTargetId())
				&& (otherMyClass.getWindowId() == this.getWindowId())) {
			return true;
		}
		else {
			return false;
		}

	}
/*
	@Override
	public int compareTo(Location other) {

		if(this.key == other.getKey()) {

			if (this.getTargetId() < other.getTargetId()) {
				return -1;
			}
			else if (this.getTargetId() > other.getTargetId()) {
				return 1;
			}
			else if (this.getTargetId() == other.getTargetId()) {

				if (this.getWindowId() < other.getWindowId()) {
					return -1;
				}
				else if (this.getWindowId() > other.getWindowId()) {
					return 1;
				}
				else {
					return 0;
				}

			}
		}
		else if (this.key < other.getKey()) {
			return -1;
		}
		else {
			return 1;
		}
		return 0;

	}
*/
	@Override
	public int hashCode() {

		String codeString = this.key+"-"+this.targetId+"-"+this.windowId;

		return codeString.hashCode();


	}


}
