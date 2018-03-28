package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.Location;

import java.util.Comparator;

/**
 * This class is used to compare two locations and, in this way, a TreeMap can be used to store hits in query mode in database
 * @author Jose M. Abuin
 */
public class LocationComparator implements Comparator<Location>{

	@Override
	public int compare(Location L1, Location L2) {

		if(L1.getKey() < L2.getKey()) {
			return -1;
		}
		else if(L1.getKey() >  L2.getKey()) {
			return 1;
		}
		else{
			if(L1.getTargetId() < L2.getTargetId()) {
				return -1;
			}
			else if(L1.getTargetId() > L2.getTargetId()) {
				return 1;
			}
			else if(L1.getTargetId() == L2.getTargetId()) {

				if(L1.getWindowId() < L2.getWindowId()) {
					return -1;
				}
				else if(L1.getWindowId() >  L2.getWindowId()) {
					return 1;
				}
				else {
					return 0;
				}

			}
		}

		return 0;

	}

}
