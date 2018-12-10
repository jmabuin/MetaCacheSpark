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

package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/21/17.
 */
public class SearchInHashMap implements FlatMapFunction<Iterator<HashMap<Integer, List<LocationBasic>>>, LocationBasic> {

	private int searchedValue = 0;

	public SearchInHashMap(int searchedValue) {
		this.searchedValue = searchedValue;
	}

	@Override
	public Iterator<LocationBasic> call(Iterator<HashMap<Integer, List<LocationBasic>>> myHashMap) {

		// Theoretically there is only one HashMap per partition
		while(myHashMap.hasNext()) {

			HashMap<Integer, List<LocationBasic>> currentHashMap = myHashMap.next();

			if (currentHashMap.containsKey(this.searchedValue)) {

				return currentHashMap.get(this.searchedValue).iterator();
			}
			else {
				return new ArrayList<LocationBasic>().iterator();
			}

		}

		return new ArrayList<LocationBasic>().iterator();

	}

}
