package com.github.metacachespark;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by chema on 1/18/17.
 */
public class DatabaseRow implements Serializable {

	private Integer key;
	private ArrayList<Location> values;

	public DatabaseRow(Integer key, ArrayList<Location> values) {
		this.key = key;
		this.values =values;
	}

	public Integer getKey() {
		return key;
	}

	public void setKey(Integer key) {
		this.key = key;
	}

	public ArrayList<Location> getValues() {
		return values;
	}

	public void setValues(ArrayList<Location> values) {
		this.values = values;
	}
}
