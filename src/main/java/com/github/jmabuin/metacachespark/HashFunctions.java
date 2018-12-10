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
import cz.adamh.utils.NativeUtils;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by chema on 1/16/17.
 */
public class HashFunctions implements Serializable {

        static {
            try {
                NativeUtils.loadLibraryFromJar("/libfunctions.so");
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
	public static native int[] sequence2features(String sequence, int windowSize,  int sketchSize, int kmerSize);

}
