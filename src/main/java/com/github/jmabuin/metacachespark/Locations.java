package com.github.jmabuin.metacachespark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Locations implements Serializable{

    private Integer key;
    private List<LocationBasic> locations;

    public Locations(Integer key, List<LocationBasic> values) {
        this.key = key;
        this.locations = values;
    }

    public Locations(){
        this.locations = new ArrayList<LocationBasic>();
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public List<LocationBasic> getLocations() {
        return locations;
    }

    public void setLocations(List<LocationBasic> locations) {
        this.locations = locations;
    }
}
