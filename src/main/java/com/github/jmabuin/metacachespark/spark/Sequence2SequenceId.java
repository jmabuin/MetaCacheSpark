package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Sequence;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by chema on 2/24/17.
 */
public class Sequence2SequenceId implements Function<Sequence, String> {

	@Override
	public String call(Sequence sequence) throws Exception {
		return sequence.getIdentifier();
	}
}
