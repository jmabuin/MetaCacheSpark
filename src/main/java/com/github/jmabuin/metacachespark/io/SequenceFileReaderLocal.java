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

import com.github.jmabuin.metacachespark.EnumModes;
import com.github.jmabuin.metacachespark.HashFunctions;
import com.github.jmabuin.metacachespark.MCSConfiguration;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by chema on 2/17/17.
 */
public class SequenceFileReaderLocal implements Serializable{

    private static final Log LOG = LogFactory.getLog(SequenceFileReader.class); // LOG to show messages

    private String 							inputFile;		// File where the sequences are stored
    //private JavaSparkContext jsc;			// JavaSparkContext object to use
    private StringBuffer 					bufferHeader;	// Buffer to store sequence headers
    private StringBuffer 					bufferData;		// Buffer to store sequence data
    private StringBuffer 					bufferQuality;	// Buffer to store sequence quality (in case of FASTQ)

    private transient BufferedReader br;				// BufferedReader to read input file
    //private FSDataInputStream inputStream;	// InputStream to read input file
    private EnumModes.InputFormat 	currentFormat;	// File format, FASTQ or FASTA

    private long readedValues = 0;

    /**
     * @brief Basic builder
     */
    public SequenceFileReaderLocal(){

    }

    /**
     * @brief Builder when considering a file and a spark context
     * @param fileName The name of the file where the sequences are stored
     */
    public SequenceFileReaderLocal(String fileName, long offset) {

        // Variable initialization
        this.inputFile = fileName;
        //this.jsc = jsc;

        this.readedValues = offset;

        this.bufferData = new StringBuffer();
        this.bufferHeader = new StringBuffer();
        this.bufferQuality = new StringBuffer();

        // Try to open the filesystem (HDFS) and sequence file
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            Path hdfs_file_path = new Path(fileName);
            Path local_file_path = new Path(hdfs_file_path.getName());

            File tmp_file = new File(local_file_path.getName());

            if(!tmp_file.exists()){
                fs.copyToLocalFile(hdfs_file_path, local_file_path);
                LOG.warn("File " + local_file_path.getName() + " copied");
            }
            else {
                LOG.warn("File " + local_file_path.getName() + " already exists. Not copying.");
            }

            this.br = new BufferedReader(new FileReader(local_file_path.getName()));

            //this.br.skip(offset);


            // Obtain the sequences file format
            if (this.inputFile.endsWith(".fastq") || this.inputFile.endsWith(".fq") || this.inputFile.endsWith(".fnq")) {
                LOG.warn("FASTQ mode");
                this.currentFormat = EnumModes.InputFormat.FASTQ;
            }
            else {
                LOG.warn("FASTA mode");
                this.currentFormat = EnumModes.InputFormat.FASTA;
            }

            if(offset > 0) {
                this.skip(offset);
            }


        }
        catch (IOException e) {
            LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.");
            e.printStackTrace();
            //System.exit(1);
        }

    }

    /**
     * This function build the next sequence in the file and returns it as a new SequenceData
     * @return The new SequenceData object that represents the sequence read
     */
    public SequenceData next() {
        //LOG.warn("[JMAbuin] Call to next");
        if(this.currentFormat == EnumModes.InputFormat.FASTA) {

            // Get a new FASTA record from file
            //LOG.warn("FASTA file");
            try {
                // We read lines until we found a new header or end of file
                //LOG.warn("[JMAbuin] Processing next sequence");
                for(String line; (line = this.br.readLine()) != null; ) {

                    // Case of first header in file
                    if((line.startsWith(">")) && (this.bufferHeader.toString().isEmpty())) {
                        this.bufferHeader.append(line.subSequence(1,line.length()));
                    }
                    // Case of new header found after a new sequence data. We build the new SequenceData to return and store the new header
                    else if ((line.startsWith(">")) && (!this.bufferHeader.toString().isEmpty())) {
                        // New sequence found. Create new record, delete old header, save new header and return record
                        SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),"");
                        this.bufferHeader.delete(0, this.bufferHeader.length());
                        this.bufferData.delete(0, this.bufferData.length());

                        this.bufferHeader.append(line.subSequence(1,line.length()));
                        //LOG.warn("[JMAbuin] New header and return");
                        this.readedValues += 1;
                        return currentSequenceData;

                    }
                    // Case of new line with data
                    else {
                        this.bufferData.append(line.replace("\n", ""));
                        //this.readedValues += (line.length() + 1);
                        //LOG.warn("[JMAbuin] Data");
                    }


                }

                // At the end, if we don't have data, is because we are at the end of the file. Return null
                if(this.bufferData.toString().isEmpty() && (this.bufferHeader.toString().isEmpty())) {
                    return null;
                }

                // If we have data, build the last record and return it.
                SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),"");

                this.bufferHeader.delete(0, this.bufferHeader.length());
                this.bufferData.delete(0, this.bufferData.length());

                this.readedValues += 1;
                return currentSequenceData;
            }
            catch (IOException e) {
                LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
                e.printStackTrace();
                //System.exit(1);
            }
            catch (Exception e) {
                LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
                e.printStackTrace();
                //System.exit(1);
            }

        }
        else if (this.currentFormat == EnumModes.InputFormat.FASTQ) {
            //LOG.warn("FASTQ file");
            // Get a new FASTQ record from file
            try {

                int i = 0;
                String line;

                for(i=0; i< 4; i++) {
                    line = this.br.readLine();

                    if(line == null) {
                        return null;
                    }
                    if (i == 0) {
                        this.bufferHeader.append(line.substring(1));
                    }
                    else if (i == 1) {
                        this.bufferData.append(line);
                    }
                    else if (i == 3) {
                        this.bufferQuality.append(line);
                        this.readedValues += 1;
                        SequenceData currentSequenceData = new SequenceData(this.bufferHeader.toString(), this.bufferData.toString(),this.bufferQuality.toString());

                        this.bufferHeader.delete(0, this.bufferHeader.length());
                        this.bufferData.delete(0, this.bufferData.length());
                        this.bufferQuality.delete(0, this.bufferQuality.length());

                        return currentSequenceData;
                    }

                }

                LOG.warn("Como chega aqui??? :: " + this.bufferHeader.toString()+" :: "+ this.bufferData.toString()+" :: "+this.bufferQuality.toString() );
                // At the end, if we don't have data, is because we are at the end of the file. Return null
                if(this.bufferData.toString().isEmpty() && (this.bufferHeader.toString().isEmpty())) {
                    return null;
                }

            }
            catch (IOException e) {
                LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
                e.printStackTrace();
                //System.exit(1);
            }
            catch (Exception e) {
                LOG.error("Could not read file "+ this.inputFile+ " because of IO error in SequenceReader.next().");
                e.printStackTrace();
                //System.exit(1);
            }
        }
        //LOG.warn("NONE file");
        return null;

    }

    /**
     * Closes the buffers readers
     */
    public void close(){
        try {
            this.br.close();
            //this.inputStream.close();
        }
        catch (IOException e) {
            LOG.error("Error closing "+ this.inputFile+ " because of IO error in SequenceReader.close().");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could closing file "+ this.inputFile+ " because of error in SequenceReader.close().");
            e.printStackTrace();
            //System.exit(1);
        }

    }

    public ArrayList<Sketch> getSketch(SequenceData sequence) {


        int currentStart = 0;
        int currentEnd = MCSConfiguration.windowSize;

        ArrayList<Sketch> returnedValues = new ArrayList<Sketch>();
        //ArrayList<Location> returnedValues = new ArrayList<Location>();

        String currentWindow = "";
        int numWindows = 0;


        // We iterate over windows (with overlap)
        //while (currentEnd < sequence.getData().length()) {
        while (currentStart < (sequence.getData().length() - MCSConfiguration.kmerSize)) {
            //Sketch resultSketch = new Sketch();
            if(currentEnd > sequence.getData().length()) {
                currentEnd = sequence.getData().length();
            }

            //LOG.warn("[JMAbuin] Init: " + currentStart+" - End: "+currentEnd);

            currentWindow = sequence.getData().substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

            // Compute k-mers
            // We compute the k-mers. In C
            int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

            //for(int newValue: sketchValues) {

            //returnedValues.add(new Location(newValue, 0, numWindows));
            returnedValues.add(new Sketch(sequence.getHeader(), sequence.getData(), sketchValues));

            //}

            // We compute the k-mers


            //returnedValuesS.add(resultSketch);


            numWindows++;
            currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
            currentEnd = currentStart + MCSConfiguration.windowSize;




        }


        return returnedValues;
    }


    public static ArrayList<Sketch> getSketchStatic(SequenceData sequence) {


        int currentStart = 0;
        int currentEnd = MCSConfiguration.windowSize;

        ArrayList<Sketch> returnedValues = new ArrayList<Sketch>();
        //ArrayList<Location> returnedValues = new ArrayList<Location>();

        String currentWindow = "";
        int numWindows = 0;


        // We iterate over windows (with overlap)
        //while (currentEnd < sequence.getData().length()) {
        while (currentStart < (sequence.getData().length() - MCSConfiguration.kmerSize)) {
            //Sketch resultSketch = new Sketch();
            if(currentEnd > sequence.getData().length()) {
                currentEnd = sequence.getData().length();
            }

            //LOG.warn("[JMAbuin] Init: " + currentStart+" - End: "+currentEnd);

            currentWindow = sequence.getData().substring(currentStart, currentEnd); // 0 - 127, 128 - 255 and so on

            // Compute k-mers
            // We compute the k-mers. In C
            int sketchValues[] = HashFunctions.window2sketch32(currentWindow, MCSConfiguration.sketchSize, MCSConfiguration.kmerSize);

            //for(int newValue: sketchValues) {

            //returnedValues.add(new Location(newValue, 0, numWindows));
            returnedValues.add(new Sketch(sequence.getHeader(), sequence.getData(), sketchValues));

            //}

            // We compute the k-mers


            //returnedValuesS.add(resultSketch);


            numWindows++;
            currentStart = MCSConfiguration.windowSize * numWindows - MCSConfiguration.overlapWindow * numWindows;
            currentEnd = currentStart + MCSConfiguration.windowSize;




        }


        return returnedValues;
    }

    public long getReadedValues() {
        return readedValues;
    }

    public void skip(long readed) {


        long current_readed = 1;

        SequenceData my_data = this.next();

        while((current_readed < readed) && (my_data != null)) {

            current_readed++;
            my_data = this.next();
        }

        //this.br.skip(readed);


    }
}
