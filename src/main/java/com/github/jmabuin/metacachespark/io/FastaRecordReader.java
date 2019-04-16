/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jmabuin.metacachespark.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;


import org.apache.hadoop.mapreduce.RecordReader;

/**
 * This class reads {@literal <key, value>} pairs from an {@code InputSplit}.
 * The input file is in FASTA format and contains a single long sequence.
 * A FASTA record has a header line that is the key, and data lines
 * that are the value.
 * {@literal >header...}
 * data
 * ...
 *
 *
 * Example:
 * {@literal >Seq1}
 * TAATCCCAAATGATTATATCCTTCTCCGATCGCTAGCTATACCTTCCAGGCGATGAACTTAGACGGAATCCACTTTGCTA
 * CAACGCGATGACTCAACCGCCATGGTGGTACTAGTCGCGGAAAAGAAAGAGTAAACGCCAACGGGCTAGACACACTAATC
 * CTCCGTCCCCAACAGGTATGATACCGTTGGCTTCACTTCTACTACATTCGTAATCTCTTTGTCAGTCCTCCCGTACGTTG
 * GCAAAGGTTCACTGGAAAAATTGCCGACGCACAGGTGCCGGGCCGTGAATAGGGCCAGATGAACAAGGAAATAATCACCA
 * CCGAGGTGTGACATGCCCTCTCGGGCAACCACTCTTCCTCATACCCCCTCTGGGCTAACTCGGAGCAAAGAACTTGGTAA
 * ...
 *
 * @author Gianluca Roscigno
 *
 * @version 1.0
 *
 * @see InputSplit
 */
public class FastaRecordReader extends RecordReader<Text, Text> {

	// input data comes from lrr
	private LineRecordReader lrr = null;

	private long startByte;

	private StringBuilder buffer;

	private Text currKey;

	private Text currValue;


	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		/*
		 * We open the file corresponding to the input split and
		 * start processing it
		 */
		FileSplit split = (FileSplit) genericSplit;
		Path path = split.getPath();
		startByte = split.getStart();

		this.buffer = new StringBuilder();

		this.lrr = new LineRecordReader();
		this.lrr.initialize(genericSplit, context);

		currKey = new Text(path.toString() + "::::"+startByte);
		currValue = new Text("");


	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (!this.lrr.nextKeyValue()) {
			return false;
		}

		final String s = this.lrr.getCurrentValue().toString().trim();
		//System.out.println("nextKeyValue() s="+s);

		// Prevent empty lines
		if (s.length() != 0) {
			this.buffer.append(s);
		}

		return true;

	}

	@Override
	public void close() throws IOException {// Close the record reader.
		this.lrr.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		this.currValue.append(this.buffer.toString().getBytes(), 0, this.buffer.length());
		return currValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return this.lrr.getProgress();
	}

}