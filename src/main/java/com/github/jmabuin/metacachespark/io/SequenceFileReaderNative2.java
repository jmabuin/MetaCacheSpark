package com.github.jmabuin.metacachespark.io;

import cz.adamh.utils.NativeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;

public class SequenceFileReaderNative2 implements Serializable {

    private static final Log LOG = LogFactory.getLog(SequenceFileReaderNative2.class);

    public  SequenceFileReaderNative2(String local_file_name) {

        try {
            NativeUtils.loadLibraryFromJar("/libsequencereader2.so");
            LOG.warn("Native library loaded");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!! : "+e.getMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!!! : "+e.getMessage());
        }

        this.init(local_file_name);

    }

    //Native methods
    private native int init(String local_file_name);
    public native String next();
    public native long total();
    public native long current();
    public native void skip(long number_skiped);
    public native String get_header();
    public native String get_data();
    public native String get_quality();
    public native void close();

}
