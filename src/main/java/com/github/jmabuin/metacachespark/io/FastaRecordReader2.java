
package com.github.jmabuin.metacachespark.io;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * FastaRecordReader is a custom record reader for FASTA file formats.
 * Treats keys as offset in file and value as line.
 */
public class FastaRecordReader2 extends RecordReader<String, Text> {

    private static final Log LOG = LogFactory.getLog(FastaRecordReader2.class);
    private CompressionCodecFactory compressionCodecs = null;
    private LineRecordReader lrr = null;

    // identify (K,V) pair
    private String key = null;
    private Text value = null;
    private String file_name = null;

    private StringBuffer data = null;

    FSDataInputStream fileIn;
    Configuration job;

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        //job = context.getConfiguration();
        final Path file = split.getPath();
        this.file_name = file.getName();

        this.lrr = new LineRecordReader();
        this.lrr.initialize(genericSplit, context);

        this.data = new StringBuffer();
    }

    public boolean nextKeyValue() throws IOException {
        int count = 0;
        //boolean found = false;

        if (value == null) {
            value = new Text();
        }

        this.key = this.file_name;


        while (true) {

            if (!this.lrr.nextKeyValue()) {
                break;
            }
            else {

                final String s = this.lrr.getCurrentValue().toString().trim();
                //LOG.info("nextKeyValue() s= " + s);

                // Prevent empty lines
                if (s.length() == 0) {
                    continue;
                }


                if ((s.charAt(0) == '>') && !this.data.toString().isEmpty()) {
                    LOG.info("nextKeyValue() s= " + s);
                    this.value.set(this.data.toString());
                    this.data.delete(0, this.data.length());
                    this.data.append(s);
                    return true;
                }

                this.data.append(s);

            }


        } //end-while

        // set value
        if (!this.data.toString().isEmpty()) {
            this.value.set(this.data.toString());
            return true;
        }

        return false;


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
    public float getProgress() throws IOException{
        return this.lrr.getProgress();
    }

    public synchronized void close() throws IOException {
        this.lrr.close();
    }
}