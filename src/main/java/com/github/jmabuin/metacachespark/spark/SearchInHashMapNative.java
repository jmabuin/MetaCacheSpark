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
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/28/17.
 */
public class SearchInHashMapNative implements FlatMapFunction<Iterator<HashMultiMapNative>, LocationBasic> {

	private int searchedValue = 0;

	public SearchInHashMapNative(int searchedValue) {
		this.searchedValue = searchedValue;
	}

	@Override
	public Iterator<LocationBasic> call(Iterator<HashMultiMapNative> myHashMaps) {

		List<LocationBasic> returnValues = new ArrayList<LocationBasic>();

		// Theoretically there is only one HashMap per partition
		while(myHashMaps.hasNext()) {

			HashMultiMapNative currentHashMap = myHashMaps.next();

			int[] values = currentHashMap.get(this.searchedValue);

			if(values != null) {

				for(int i = 0; i< values.length; i+=2) {

					returnValues.add(new LocationBasic(values[i], values[i+1]));
				}

			}

		}

		return returnValues.iterator();

	}

}