package com.github.jmabuin.metacachespark;

import cz.adamh.utils.NativeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by chema on 1/16/17.
 */
public class LocationsSortedList implements Serializable {

    private static final Log LOG = LogFactory.getLog(LocationsSortedList.class);

    public LocationsSortedList() {
        try {
            NativeUtils.loadLibraryFromJar("/liblocationssortedlist.so");
            LOG.warn("Native library loaded");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!! : "+e.getMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!! : "+e.getMessage());
        }

        this.init();
    }


    private native int init();
    public native int add(int tgt, int win);
    public native int[] get(int key);
    public native int size();


    public LocationBasic[] get_locations() {

        LocationBasic[] return_locations = new LocationBasic[this.size()];

        for(int i = 0; i< this.size(); ++i) {

            int current_values[] = this.get(i);
            LocationBasic current = new LocationBasic(current_values[0], current_values[1]);
            return_locations[i] = current;
        }


        return return_locations;

    }

    public LocationBasic get_location(int i) {

        int values[] = this.get(i);

        return new LocationBasic(values[0], values[1]);

    }

}
