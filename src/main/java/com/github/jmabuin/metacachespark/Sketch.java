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
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;

import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Sketch implements Serializable {

    private int[] features;
    //private String header;
    //private String sequence;

    public Sketch(MetaCacheOptions options) {

        this.features = new int[options.getProperties().getSketchlen()];


    }

	//public Sketch(String header, String sequence, int[] features) {
	public Sketch(int[] features) {

		this.features = features;
		//this.header = header;
		//this.sequence = sequence;

	}

    public int[] getFeatures() {
        return features;
    }

    public void setFeatures(int[] features) {
        this.features = features;
    }

    /*
	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}
*/
}
