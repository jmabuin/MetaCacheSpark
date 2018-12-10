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
//import org.apache.hadoop.io.LongWritable;
import com.github.jmabuin.metacachespark.Sequence;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * This class define a custom RecordReader for FASTA files for the
 * Hadoop MapReduce framework.
 * @author Mahmoud Parsian
 * @author José M. Abuín
 */
public class Fasta2SequenceRecordReader extends RecordReader<String, Sequence> {

    // input data comes from lrr
    private LineRecordReader lrr = null;

    private StringBuffer key = new StringBuffer();
    private StringBuffer value = new StringBuffer();

    private Sequence current_sequence = new Sequence();
    private StringBuffer previousKey = new StringBuffer();

    @Override
    public void close() throws IOException {
        this.lrr.close();
    }

    @Override
    public String getCurrentKey()
            throws IOException, InterruptedException {
        return key.toString();
    }

    @Override
    public Sequence getCurrentValue()
            throws IOException, InterruptedException {
        return current_sequence;
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException {
        return this.lrr.getProgress();
    }

    @Override
    public void initialize(final InputSplit inputSplit,
                           final TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        this.lrr = new LineRecordReader();
        this.lrr.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        int count = 0;
        boolean found = false;

        this.value.delete(0, this.value.length());
        this.key.delete(0, this.key.length());

        while (!found) {

            if (!this.lrr.nextKeyValue()) {
                return false;
            }

            final String s = this.lrr.getCurrentValue().toString().trim();
            //System.out.println("nextKeyValue() s="+s);

            // Prevent empty lines
            if (s.length() == 0) {
                continue;
            }

            long currentkey = this.lrr.getCurrentKey().get();

            if(s.startsWith(">")) { // New header

                if(this.value.toString().isEmpty()) { // New record
                    this.key.append(s.substring(1));
                    this.value.append(s);
                    this.value.append("\n");
                }
                else {
                    this.previousKey.append(s);
                    found = true;
                }

            }
            else {

                if(!this.previousKey.toString().isEmpty()) {
                    this.key.append(this.previousKey.toString());
                    this.value.append(this.previousKey.toString());
                    this.value.append("\n");
                    this.previousKey.delete(0, this.previousKey.length());
                }

                this.value.append(s);
                this.value.append("\n");
            }

        } //end-while

        return true;
    }

}