package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

/**
 * Created by chema on 3/4/17.
 */
public class Row2Location implements MapFunction<Row, Location> {

	@Override
	public Location call(Row row) {
		return new Location(row.getInt(0), row.getInt(1), row.getInt(2));
	}
}
