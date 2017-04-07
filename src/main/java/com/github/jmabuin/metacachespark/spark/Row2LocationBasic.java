package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by chema on 3/4/17.
 */
public class Row2LocationBasic implements Function<Row, LocationBasic> {

	@Override
	public LocationBasic call(Row row) {
		return new LocationBasic(row.getInt(1), row.getInt(2));
	}
}
