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

package com.github.jmabuin.metacachespark.io;

import java.io.Serializable;

/**
 * This class encapsulates the data from a Sequence
 * @author Jose M. Abuin
 */
public class SequenceData implements Serializable {
	private String header;	// The sequence header
	private String data;	// the sequence data
	private String quality; // The sequence quality (Only in the FASTQ case)

	/**
	 * Builder
	 * @param header A String containing the header
	 * @param data A String containing the data
	 * @param quality A String containing the quality
	 */
	public SequenceData(String header, String data, String quality) {
		this.header = header;
		this.data = data;
		this.quality = quality;
	}

	/**
	 * Getter for the header
	 * @return A String containing the header
	 */
	public String getHeader() {
		return header;
	}

	/**
	 * Setter for the header
	 * @param header A String containing the new header
	 */
	public void setHeader(String header) {
		this.header = header;
	}

	/**
	 * Getter for the data
	 * @return A String containing the data
	 */
	public String getData() {
		return data;
	}

	/**
	 * Setter for the data
	 * @param data A String containing the new data for this sequence
	 */
	public void setData(String data) {
		this.data = data;
	}

	/**
	 * Getter for the quality
	 * @return A String containing the quality
	 */
	public String getQuality() {
		return quality;
	}

	/**
	 * Setter for the quality
	 * @param quality A String containing the new quality
	 */
	public void setQuality(String quality) {
		this.quality = quality;
	}
}
