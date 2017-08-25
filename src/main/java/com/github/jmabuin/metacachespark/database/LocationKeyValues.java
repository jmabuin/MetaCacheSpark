package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.LocationBasic;

import java.io.Serializable;
import java.util.List;

/**
 * Created by chema on 4/21/17.
 */
public class LocationKeyValues implements Serializable {


    private int key;
    //private List<LocationBasic> values;
    private String values;

    public LocationKeyValues(int key, String values) {
        this.key = key;
        this.values = values;
    }


    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }
}
