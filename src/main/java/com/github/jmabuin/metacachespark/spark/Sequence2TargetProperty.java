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

		return new TargetProperty(arg0.getSeqId(), arg0.getTaxid(), arg0.getSequenceOrigin());
		//return new TargetProperty(arg0.getIdentifier(), sequencesIndexes.get(arg0.getIdentifier()), arg0.getSequenceOrigin());

	}
}
