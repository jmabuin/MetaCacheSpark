package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.Location;

import java.util.Comparator;

/**
 * Created by chema on 2/10/17.
 */
public class LocationComparator implements Comparator<Location>{


	public int compare(Location L1, Location L2) {

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

		return 0;

	}

}
