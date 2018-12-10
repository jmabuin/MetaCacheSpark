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
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * FastaRecordReader is a custom record reader for FASTA file formats.
 * Treats keys as offset in file and value as line.
 */
public class FastaRecordReader extends RecordReader<String, Text> {

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;

	// identify (K,V) pair
	private String key = null;
	private Text value = null;
	private String file_name;

	FSDataInputStream fileIn;
	Configuration job;

	public void initialize(InputSplit genericSplit,
						   TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		this.file_name = file.getName();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) {
			// skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int)Math.min((long)Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = this.file_name;
		}
		//key.set(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize;

		StringBuilder text = new StringBuilder();
		int recordLength = 0;
		Text line = new Text();
		int recordsRead = 0;
		while (pos < end) {
			//key.set(pos);
			newSize = in.readLine(line, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));

			if(line.toString().indexOf(">") >= 0) {// && (!text.toString().isEmpty())){
				if(recordsRead > 9){//10 fasta records each time
					value.set(text.toString());
					fileIn.seek(pos);
					in = new LineReader(fileIn, job);
					return true;
				}
				recordsRead++;
			}

			recordLength += newSize;
			text.append(line.toString());
			text.append("\n");
			pos += newSize;

			if (newSize == 0) {
				break;
			}
		}
		if (recordLength == 0){
			return false;
		}
		value.set(text.toString());
		return true;

	}

	@Override
	public String getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

/*
	public boolean nextKeyValue() throws IOException {
		//if (key == null) {
			key = this.file_name;
		//}
		//key.set(pos);
		//if (value == null) {
			value = new Text();
		//}
		int newSize;

		StringBuilder text = new StringBuilder();
		int recordLength = 0;
		Text line = new Text();
		//int recordsRead = 0;
		while (pos < end) {
			//key.set(pos);
			newSize = in.readLine(line, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));

			if((line.toString().indexOf(">") >= 0) && (!text.toString().isEmpty())){
				//if(recordsRead > 9){//10 fasta records each time
				value.set(text.toString());
				fileIn.seek(pos);
				in = new LineReader(fileIn, job);
				return true;
				//}
				//recordsRead++;
			}

			recordLength += newSize;
			text.append(line.toString());
			text.append("\n");
			pos += newSize;

			if (newSize == 0) {
				break;
			}
		}
		if (recordLength == 0){
			return false;
		}
		value.set(text.toString());
		return true;

	}
*/


	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		}
		else {
			return Math.min(1.0f, (pos - start) / (float)(end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}