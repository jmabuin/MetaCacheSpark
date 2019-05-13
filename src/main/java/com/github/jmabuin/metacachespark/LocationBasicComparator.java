package com.github.jmabuin.metacachespark;

import java.io.Serializable;
import java.util.Comparator;

public class LocationBasicComparator implements Comparator<LocationBasic>, Serializable {
    @Override
    public int compare(LocationBasic o, LocationBasic t1) {

        if (o.getTargetId() > t1.getTargetId()) {
            return 1;
        }

        if (o.getTargetId() < t1.getTargetId()) {
            return -1;
        }

        if (o.getWindowId() > t1.getWindowId()) {
            return 1;
        }

        if (o.getWindowId() < t1.getWindowId()) {
            return -1;
        }

        return 0;

    }
}
