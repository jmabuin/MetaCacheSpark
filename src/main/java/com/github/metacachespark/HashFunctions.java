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
		x = ((x >> 16) ^ x) * 0x45d9f3b;
		x = ((x >> 16) ^ x) * 0x45d9f3b;
		x = ((x >> 16) ^ x);
		return x;
	}

	public static int make_reverse_complement(int s, byte k) {

		s = ((s >> 2)  & 0x33333333) | ((s & 0x33333333) << 2);
		s = ((s >> 4)  & 0x0F0F0F0F) | ((s & 0x0F0F0F0F) << 4);
		s = ((s >> 8)  & 0x00FF00FF) | ((s & 0x00FF00FF) << 8);
		s = ((s >> 16) & 0x0000FFFF) | ((s & 0x0000FFFF) << 16);
		return (0xFFFFFFFF - s) >> (8 * 4 - (k << 1));
	}

	public static String reverse_complement(String str) {

		char[] reversedStringArray = new StringBuilder(str).reverse().toString().toCharArray();

		int i = 0;
		for(char c : reversedStringArray) {
			switch(c) {
				case 'A': reversedStringArray[i] = 'T'; break;
				case 'a': reversedStringArray[i] = 't'; break;
				case 'C': reversedStringArray[i] = 'G'; break;
				case 'c': reversedStringArray[i] = 'g'; break;
				case 'G': reversedStringArray[i] = 'C'; break;
				case 'g': reversedStringArray[i] = 'c'; break;
				case 'T': reversedStringArray[i] = 'A'; break;
				case 't': reversedStringArray[i] = 'a'; break;
				default: break;
			}
		i++;
		}

		return new String(reversedStringArray);

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

}
