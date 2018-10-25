package com.github.jmabuin.metacachespark;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LocationsType implements Serializable{

    private Integer key;
    private ArrayType locations;

    public LocationsType(Integer key, ArrayType values) {
        this.key = key;
        this.locations = values;
    }

    public LocationsType(){
        ArrayType items = DataTypes.createArrayType(new IntegerType());
        this.locations = DataTypes.createArrayType(items);
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public ArrayType getLocations() {
        return locations;
    }

    public void setLocations(ArrayType locations) {
        this.locations = locations;
    }
}
