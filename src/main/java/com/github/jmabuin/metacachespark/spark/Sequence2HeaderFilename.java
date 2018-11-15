package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Sequence;
import com.github.jmabuin.metacachespark.SequenceHeaderFilename;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by chema on 2/24/17.
 */
public class Sequence2HeaderFilename implements Function<Sequence, SequenceHeaderFilename> {

    @Override
    public SequenceHeaderFilename call(Sequence sequence) throws Exception {
        return new SequenceHeaderFilename(sequence.getHeader(), sequence.getSequenceOrigin().getFilename());
    }
}
