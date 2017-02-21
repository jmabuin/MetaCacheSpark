package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;


/**
 * Created by chema on 2/17/17.
 */
public class Row2Location implements Function<Row, Location> {

	@Override
	public Location call(Row row) {
		return new Location(row.getInt(0), row.getInt(1), row.getInt(2));
	}

}
