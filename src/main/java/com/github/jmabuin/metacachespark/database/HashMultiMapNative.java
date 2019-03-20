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

package com.github.jmabuin.metacachespark.database;

import com.github.jmabuin.metacachespark.LocationBasic;
import cz.adamh.utils.NativeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by chema on 3/23/17.
 */
public class HashMultiMapNative implements Serializable {

    private static final Log LOG = LogFactory.getLog(HashMultiMapNative.class);

    public HashMultiMapNative(int max_size) {

        try {
            NativeUtils.loadLibraryFromJar("/libmetacache.so");
            LOG.warn("Native library loaded");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!! : "+e.getMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!! : "+e.getMessage());
        }

        this.init(max_size);
    }

    private native int init(int number);
    public native int add(int key, int value1, int value2);
    public native int[] get(int key);
    public native int[] keys();
    public native int size();
    public native void write(String fileName);
    public native void read(String fileName);
    public native int post_process(boolean over_populated, boolean ambiguous);
    public native void clear();
    public native void clear_key(int key);

    public LocationBasic[] get_locations(int key) {

        int values[] = this.get(key);


        if(values != null) {
            int size_locations = values.length / 2;

            int i,j;
            LocationBasic[] return_locations = new LocationBasic[size_locations];

            for (  i = 0, j = 0; (i< values.length) && (j<return_locations.length); i+=2, ++j) {

                return_locations[j] = new LocationBasic(values[i], values[i+1]);
                //return_locations[j].setTargetId(values[i]);
                //return_locations[j].setWindowId(values[i+1]);

            }

            return return_locations;
        }

        return null;




    }

}
