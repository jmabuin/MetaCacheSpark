/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

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
