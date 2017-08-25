package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;

import java.io.Serializable;
import java.util.Comparator;

/**
 * This class is used to compare two locations and, in this way, a TreeMap can be used to store hits in query mode in database
 * @author Jose M. Abuin
 */
public class LocationBasicComparator implements Serializable, Comparator<LocationBasic>{

	@Override
	public int compare(LocationBasic L1, LocationBasic L2) {

		if(L1.getTargetId() < L2.getTargetId()) {
			return -1;
		}
		if(L1.getTargetId() > L2.getTargetId()) {
			return 1;
		}

		if(L1.getWindowId() < L2.getWindowId()) {
				return -1;
		}
		if(L1.getWindowId() >  L2.getWindowId()) {
				return 1;
		}


		return 0;

	}

}
