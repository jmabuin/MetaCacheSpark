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

import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.TargetProperty;
import org.apache.spark.api.java.function.Function;

import java.util.HashMap;

/**
 * Created by chema on 2/22/17.
 */
public class Sequence2TargetProperty implements Function<Sequence, TargetProperty> {



	@Override
	public TargetProperty call(Sequence arg0) {

		return new TargetProperty(arg0.getSeqId(), arg0.getTaxid(), arg0.getSequenceOrigin(), arg0.getHeader());
		//return new TargetProperty(arg0.getIdentifier(), sequencesIndexes.get(arg0.getIdentifier()), arg0.getSequenceOrigin());

	}
}
