package com.github.jmabuin.metacachespark.database;

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

}
