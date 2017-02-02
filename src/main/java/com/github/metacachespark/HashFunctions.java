package com.github.metacachespark;

import cz.adamh.utils.NativeUtils;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by chema on 1/16/17.
 */
public class HashFunctions implements Serializable {

        static {
            try {
                NativeUtils.loadLibraryFromJar("/libmetacache.so");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

	public static int thomas_mueller_hash(int x) {
		return thomas_mueller_hash32(x);
	}


	public static int make_canonical(int s, int k) {
		int revcom = make_reverse_complement32(s, k);
		return s < revcom ? s : revcom;
	}

	public static long make_canonical(long s, int k) {
		long revcom = make_reverse_complement64(s, k);
		return s < revcom ? s : revcom;
	}


	//Native methods
	public static native long make_reverse_complement64(long s, int k);
	public static native int make_reverse_complement32(int s, int k);
	public static native int thomas_mueller_hash32(int x);
	public static native int kmer2uint32(String s);
	public static native int[] window2sketch32(String window, int sketchSize, int kmerSize);

}
