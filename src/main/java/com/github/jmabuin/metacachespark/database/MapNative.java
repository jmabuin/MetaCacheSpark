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

import cz.adamh.utils.NativeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chema on 5/29/17.
 */
public class MapNative implements Serializable {

    private static final Log LOG = LogFactory.getLog(MapNative.class);

    public MapNative() {

        try {
            NativeUtils.loadLibraryFromJar("/libmymap.so");
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
    public native int add(int tgt, int win, int value);
    public native int addAll(int[] data);
    public native int get(int key, int win);
    public native int[] get_best(int size);
    public native int[] get_by_pos(int pos);
    public native int size();
    public native void clear();
    public native void resetIterator();
    public native boolean isEmpty();
    public native int[] get_current();
    public native void next();
    public native boolean isEnd();

    public native void resetFSTIterator();
    public native void resetLSTIterator();
    public native int[] get_currentFST();
    public native int[] get_currentLST();
    public native void nextFST();
    public native void nextLST();
    public native boolean isEndFST();
    public native boolean isEndLST();
    public native void setFST2LST();


    public List<int[]> toList() {

        List<int[]> returnedItems = new ArrayList<int[]>();

        this.resetIterator();

        while(!this.isEnd()) {

            int currentItem[] = this.get_current();

            returnedItems.add(currentItem);

            this.next();

        }

        return returnedItems;

    }

}
