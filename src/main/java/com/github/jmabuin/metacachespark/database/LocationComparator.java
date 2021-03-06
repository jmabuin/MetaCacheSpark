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
