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

package com.github.jmabuin.metacachespark;
import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
//public class LocationBasic implements Serializable, Comparable<Location>{
public class LocationBasic implements Serializable, Comparable<LocationBasic> {

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
		//if (other == this) return true;
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

	@Override
	public int hashCode() {

		return (this.targetId+"-"+this.windowId).hashCode();

	}

	@Override
	public int compareTo(LocationBasic other){

		if (other == null) return 1;

		if(this.getTargetId() < other.getTargetId()) {
			return -1;
		}
		if(this.getTargetId() > other.getTargetId()) {
			return 1;
		}

		if(this.getWindowId() < other.getWindowId()) {
			return -1;
		}
		if(this.getWindowId() > other.getWindowId()) {
			return 1;
		}


		return 0;
	}


}
