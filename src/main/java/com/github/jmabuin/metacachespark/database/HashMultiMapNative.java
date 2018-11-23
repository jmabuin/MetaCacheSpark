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

	public HashMultiMapNative() {

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

		this.init();
	}

	private native int init();
	public native int add(int key, int value1, int value2);
	public native int[] get(int key);
	public native int size();
	public native void write(String fileName);
	public native void read(String fileName);

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
