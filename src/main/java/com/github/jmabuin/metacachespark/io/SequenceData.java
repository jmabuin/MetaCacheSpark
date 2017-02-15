package com.github.jmabuin.metacachespark.io;

/**
 * Created by chema on 2/14/17.
 */
public class SequenceData {
	private String header;
	private String data;
	private String quality;

	public SequenceData(String header, String data, String quality) {
		this.header = header;
		this.data = data;
		this.quality = quality;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getQuality() {
		return quality;
	}

	public void setQuality(String quality) {
		this.quality = quality;
	}
}
