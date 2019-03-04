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


import com.github.jmabuin.metacachespark.database.*;
import com.github.jmabuin.metacachespark.io.*;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;


public class Query implements Serializable {

    private static final Log LOG = LogFactory.getLog(Query.class);

    // Default options values
    private MetaCacheOptions param;

    private Database db;
    private JavaSparkContext jsc;

    private EnumModes.InputFormat inputFormat;
    private HashMap<Location, Integer> hits;

    public Query(MetaCacheOptions param, JavaSparkContext jsc) {

        this.param = param;

        this.jsc = jsc;
        LOG.info("Building query object ...");
        this.db = new Database(jsc, this.param.getDbfile(), this.param);

        /*if(this.param.getMaxLoadFactor() > 0) {
            this.db.setmaxloadfactor
        }

        if(this.param.getMaxTargetsPerSketchVal() > 1) {
            db.remove_features_with_more_locations_than(param.maxTargetsPerSketchVal);
        }*/

        if(this.param.getProperties().getHitsMin() < 1) {
            int sks = MCSConfiguration.sketchSize;

            if(sks >= 6) {
                this.param.getProperties().setHitsMin((int) (sks/3.0));
            }
            else if (sks >= 4) {
                this.param.getProperties().setHitsMin(2);
            }
            else {
                this.param.getProperties().setHitsMin(1);
            }
        }

        this.hits = new HashMap<Location, Integer>();

        this.query();
    }

    public void query() {


        StringBuffer outfile = new StringBuffer();

        //process files / file pairs separately
        if(this.param.getProperties().isSplitOutput()) {
            //std::string outfile;
            //process each input file pair separately
            if(this.param.getProperties().getPairing() == EnumModes.pairing_mode.files && this.param.getInfiles_query().length > 1) {

                LOG.info("Processing file pairs");

                for(int i = 0; i < this.param.getInfiles_query().length; i += 2) {
                    String f1 = this.param.getInfiles_query()[i];
                    String f2 = this.param.getInfiles_query()[i+1];

                    if(!this.param.getOutfile().isEmpty()) {
                        outfile.delete(0,outfile.length());

                        int index1 = f1.lastIndexOf(File.separator);
                        String fileName1 = f1.substring(index1 + 1);

                        int index2 = f2.lastIndexOf(File.separator);
                        String fileName2 = f1.substring(index2 + 1);

                        outfile.append(this.param.getOutfile());
                        outfile.append("_");
                        outfile.append(fileName1);
                        outfile.append("_");
                        outfile.append(fileName2);

                    }

                    //process_input_files(db, param, std::vector<std::string>{f1,f2}, outfile);
                    String[] currentInput = {f1, f2};
                    this.process_input_files(currentInput, outfile.toString());
                }
            }
            //process each input file separately
            else {
                LOG.info("Processing each file separately");
                for(String f : this.param.getInfiles_query()) {
                    if(!this.param.getOutfile().isEmpty()) {
                        outfile.delete(0,outfile.length());
                        outfile.append(this.param.getOutfile());
                        outfile.append("_");

                        int index = f.lastIndexOf(File.separator);
                        String fileName = f.substring(index + 1);

                        outfile.append(fileName);
                        outfile.append(".txt");

                    }
                    //process_input_files(db, param, std::vector<std::string>{f}, outfile);
                    String[] currentInput = {f};
                    this.process_input_files(currentInput, outfile.toString());
                }
            }
        }
        //process all input files at once
        else {
            outfile.delete(0,outfile.length());
            outfile.append(this.param.getOutfile());

            //process_input_files(db, param, param.infiles, param.outfile);
            this.process_input_files(this.param.getInfiles_query(), outfile.toString());
        }
    }


    public void process_input_files(String[] inputfiles, String outputfile) {

        // classify_sequences(db, param, infilenames, os);
        // Open output buffer

        try {
            FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(outputfile), true);

            BufferedWriter d = new BufferedWriter(new OutputStreamWriter(outputStream));

            this.classify_sequences(inputfiles, d);


            d.close();
            outputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            LOG.error("I/O Error accessing HDFS in process_input_files: "+e.getMessage());
            System.exit(1);
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error accessing HDFS in process_input_files: "+e.getMessage());
            System.exit(1);
        }



    }

    public void classify_sequences(String[] infilenames, BufferedWriter d) {
        try {

            StringBuffer comment = new StringBuffer();

            if(this.param.getProperties().getMapViewMode() != EnumModes.map_view_mode.none) {
                comment.append("Reporting per-read mappings (non-mapping lines start with '').\n");
                comment.append("Output will be constrained to ranks from 'taxonomy::rank_name(param.lowestRank)' to" +
                        "'taxonomy::rank_name(param.highestRank)'.\n");

                if(this.param.getProperties().isShowLineage()) {
                    comment.append("The complete lineage will be reported "
                            + "starting with the lowest match.\n");
                }
                else {
                    comment.append("Only the lowest matching rank will be reported.\n");
                }
            }
            else {
                comment.append("Per-Read mappings will not be shown.\n");
            }

            if(this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none) {
                comment.append("Clade Exclusion on Rank: " +
                        Taxonomy.rank_name(this.param.getProperties().getExcludedRank()));
            }

            if(this.param.getProperties().getPairing() == EnumModes.pairing_mode.files) {
                comment.append("File based paired-end mode:\n" +
                        "  Reads from two consecutive files will be interleaved.\n" +
                        "  Max insert size considered " + this.param.getProperties().getInsertSizeMax() + ".\n");
            }
            else if(this.param.getProperties().getPairing() == EnumModes.pairing_mode.sequences) {
                comment.append("Per file paired-end mode:\n"
                        + "  Reads from two consecutive sequences in each file will be paired up.\n"
                        + "  Max insert size considered \" + this.param.getInsertSizeMax() + \".\n");
            }

            if(this.param.getProperties().isTestAlignment()) {
                comment.append("Query sequences will be aligned to best candidate target => SLOW!\\n");
            }

            comment.append("Using "+this.param.getNumThreads() + " threads\n");

            long initTime = System.nanoTime();

            ClassificationStatistics stats = new ClassificationStatistics();

            //if(this.param.getProperties().getPairing() == EnumModes.pairing_mode.files) {
            if (this.param.isPaired_reads()) {
                classify_on_file_pairs(infilenames, d, stats);
            }
            else {
                classify_per_file(infilenames, d, stats);
            }

            long endTime = System.nanoTime();

            //show results
            int numQueries = (this.param.getProperties().getPairing() == EnumModes.pairing_mode.none) ? stats.total() :
                    2 * stats.total();

            double speed = (60.0 * (double)numQueries) / ((double)(endTime - initTime)/1e9);

            comment.append("queries:       " + numQueries + "\n");
            //comment.append("basic queries: " + stats.total()+"\n");
            comment.append("time:          " + (endTime - initTime)/1e6 + " ms\n");
            comment.append("speed:         " + speed + " queries/min\n");

            if(stats.total() > 0) {
                //show_classification_statistics(os, stats, comment);
                this.show_classification_statistics(d, stats, comment);
            }
            else {
                comment.append("No valid query sequences found.\n");
                d.write(comment.toString());
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_sequences: "+e.getMessage());
            System.exit(1);
        }


    }

    public void show_classification_statistics(BufferedWriter d, ClassificationStatistics stats, StringBuffer prefix) {

        try {
            Taxonomy.Rank ranks[] = {Taxonomy.Rank.Sequence, Taxonomy.Rank.subSpecies, Taxonomy.Rank.Species,
                    Taxonomy.Rank.Genus, Taxonomy.Rank.Family, Taxonomy.Rank.Order, Taxonomy.Rank.Class,
                    Taxonomy.Rank.Phylum, Taxonomy.Rank.Kingdom, Taxonomy.Rank.Domain};

            if(stats.assigned() < 1) {
                prefix.append("None of the input sequences could be classified.\n");
            }

            if(stats.unassigned() > 0) {
                prefix.append("unclassified: " + (100 * stats.unclassified_rate()) + "% (" + stats.unassigned() + ")\n");
            }

            prefix.append("classified:\n");

            for(Taxonomy.Rank r: ranks) {
                if(stats.assigned(r) > 0) {
                    String rn = Taxonomy.rank_name(r);
                    //rn.resize(11, ' ');
                    prefix.append(" " + rn +"\t" + (100 * stats.classification_rate(r)) + "% (" + stats.assigned(r) + ")\n");
                }
            }

            if(stats.known() > 0) {
            //if(this.param.getProperties().isTestPrecision()) {
                if(stats.unknown() > 0) {
                    prefix.append("ground truth unknown: " + (100 * stats.unknown_rate()) + "% (" + stats.unknown() + ")\n");
                }

                prefix.append("ground truth known:\n");

                for(Taxonomy.Rank r: ranks) {
                    if(stats.assigned(r) > 0) {
                        String rn = Taxonomy.rank_name(r);
                        //rn.resize(11, ' ');
                        prefix.append(" " + rn +"\t"+ (100 * stats.known_rate(r)) + "% (" + stats.known(r) + ")\n");
                    }
                }

                prefix.append("correctly classified:\n");
                for(Taxonomy.Rank r: ranks) {
                    if(stats.assigned(r) > 0) {
                        String rn = Taxonomy.rank_name(r);
                        //rn.resize(11, ' ');
                        prefix.append(" " + rn +"\t" + stats.correct(r) + "\n");
                    }
                }

                prefix.append("precision (correctly classified / classified):\n");
                for(Taxonomy.Rank r: ranks) {
                    if(stats.assigned(r) > 0) {
                        String rn = Taxonomy.rank_name(r);
                        //rn.resize(11, ' ');
                        prefix.append(" " + rn +"\t" + (100 * stats.precision(r)) + "\n");
                    }
                }

                prefix.append("sensitivity (correctly classified / all):\n");
                for(Taxonomy.Rank r: ranks) {
                    if(stats.assigned(r) > 0) {
                        String rn = Taxonomy.rank_name(r);
                        //rn.resize(11, ' ');
                        prefix.append(" " + rn +"\t" + (100 * stats.sensitivity(r)) + "\n");
                    }
                }

                if (stats.coverage(Taxonomy.Rank.Domain).total() > 0) {
                    prefix.append("false positives (hit on taxa not covered in DB):\n");

                    for(Taxonomy.Rank r: ranks) {
                        if(stats.assigned(r) > 0) {
                            String rn = Taxonomy.rank_name(r);
                            //rn.resize(11, ' ');
                            prefix.append(" " + rn +"\t" + stats.coverage(r).false_pos() + "\n");
                        }
                    }


                }

            }

            d.write(prefix.toString());
        }
        catch (IOException e) {
            e.printStackTrace();
            LOG.error("IO exception error in show_classification_statistics: "+e.getMessage());
            System.exit(1);
        }




    }



    public void classify_on_file_pairs(String[] infilenames, BufferedWriter d, ClassificationStatistics stats) {


        //pair up reads from two consecutive files in the list
        for(int i = 0; i < infilenames.length; i += 2) {
            String fname1 = infilenames[i];
            String fname2 = infilenames[i+1];

            LOG.warn("Classifying file pairs on " + fname1 + " and " + fname2 );
            this.classify_pairs(fname1, fname2, d, stats);
            //this.classify_pairs_list(fname1, fname2, d, stats);
            //this.classify_pairs_best(fname1, fname2, d, stats);
            //this.classify_pairs_main(fname1, fname2, d, stats);
        }

    }

    public void classify_per_file(String[] infilenames, BufferedWriter d, ClassificationStatistics stats) {


        try {

            for(int i = 0; i < infilenames.length; i++) {
                String fname = infilenames[i];

                LOG.warn("Processing file "+fname);

                this.classify(fname, d, stats);

            }
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_per_file: "+e.getMessage());
            System.exit(1);
        }

    }


    public void classify_pairs(String f1, String f2, BufferedWriter d, ClassificationStatistics stats) {

        LOG.warn("Entering classify_pairs");
        long initTime = System.nanoTime();

        try {

            long totalReads = 0;
            long totalReads2 = 0;

            if (FilesysUtility.isFastaFile(f1) && FilesysUtility.isFastaFile(f2)) {
                totalReads = FilesysUtility.readsInFastaFile(f1);
                totalReads2 = FilesysUtility.readsInFastaFile(f1);

                LOG.warn("Number of reads in " + f1 + " is " + totalReads +
                        ", while number of reads in " + f2 + " is " + totalReads2 + ".");

                if (totalReads != totalReads2) {
                    System.exit(1);
                }

            }
            else if (FilesysUtility.isFastqFile(f1) && FilesysUtility.isFastqFile(f2)) {
                totalReads = FilesysUtility.readsInFastqFile(f1);
                totalReads2 = FilesysUtility.readsInFastqFile(f1);

                LOG.warn("Number of reads in " + f1 + " is " + totalReads +
                        ", while number of reads in " + f2 + " is " + totalReads2 + ".");
                if (totalReads != totalReads2) {

                    System.exit(1);
                }
            }
            else {
                LOG.error("Not recognized file format in " + f1 + " and " + f2);
                System.exit(1);
            }


            long startRead;
            int bufferSize = this.param.getBuffer_size();

            SequenceFileReaderLocal seqReader = new SequenceFileReaderLocal(f1, 0);
            SequenceFileReaderLocal seqReader2 = new SequenceFileReaderLocal(f2, 0);

            LOG.info("Sequence reader created. Current index: " + seqReader.getReadedValues());

            SequenceData data;
            SequenceData data2;

            for(startRead = 0; startRead < totalReads; startRead+=bufferSize) {
                //while((currentRead < startRead+bufferSize) && ) {

                LOG.warn("Parsing new reads block. Starting in: "+startRead + " and ending in  " + (startRead + bufferSize));


                // Get corresponding hits for this buffer
                //List<List<MatchCandidate>> hits = this.db.accumulate_matches_native_buffered_best(f1, f2,
                //        startRead, bufferSize);
                Map<Long, List<MatchCandidate>> hits = this.db.accumulate_matches_paired(f1, f2,
                                startRead, bufferSize);

                LOG.warn("Results in buffer: " + hits.size() + ". Buffer size is:: "+bufferSize);

                //for(long i = 0;  (i < totalReads) && (i < currentRead + bufferSize); i++) {

                //LocationBasic current_key;

                long current_read;

                for(long i = 0;  i < hits.size() ; i++) {

                    current_read = startRead + i;
                    //Theoretically, number of sequences in data is the same as number of hits
                    data = seqReader.next();
                    data2 = seqReader2.next();

                    if((i == 0) || (i == hits.size()-1)) {
                        LOG.warn("Read " + i + " is " + data.getHeader() + " :: " + data.getData());
                    }

                    if((data == null) || (data2 == null)) {
                        LOG.warn("Data is null!! for hits: " + i + " and read " + (startRead + i));
                        break;
                    }

                    List<MatchCandidate> currentHits = hits.get(current_read);

                    this.process_database_answer(data.getHeader(), data.getData(),
                            data2.getData(), currentHits, d, stats);

                }




            }

            seqReader.close();
            seqReader2.close();

            long endTime = System.nanoTime();

            LOG.warn("Time in classify_pairs_best is: " + ((endTime - initTime) / 1e9) + " seconds");
            //LOG.warn("Total characters readed: " + seqReader.getReadedValues());

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_pairs: "+e.getMessage());
            System.exit(1);
        }

    }
/*
    public void classify_pairs_main(String fname1, String fname2, BufferedWriter d, ClassificationStatistics stats) {

        switch (this.param.getDatabase_type()) {

            case HASHMULTIMAP_NATIVE:
                LOG.info("Classifying per file with native hashmap buffered");
                this.classify_pairs(fname1, fname2, d, stats);

                break;
            case HASHMAP:

                break;
            case HASHMULTIMAP_GUAVA:

                break;
            default:
                //this.classify(filename, d, stats);
                break;

        }


    }
*/
/*
    public void classify_main(String filename, BufferedWriter d, ClassificationStatistics stats){

        switch (this.param.getDatabase_type()) {

            case HASHMULTIMAP_NATIVE:
                if (this.param.getBuffer_size() > 0) {
                    LOG.info("Classifying per file with native hashmap buffered");
                    this.classify(filename, d, stats);
                }
                else if (this.param.getBuffer_size() == 0) {
                    LOG.info("Classifying per file with native hashmap single");
                    this.classify_native_single(filename, d, stats);
                }
                break;
            case HASHMAP:
                if (this.param.getBuffer_size() > 0) {
                    LOG.info("Classifying per file with native hashmap buffered");
                    this.classify_java_hashmap_buffered(filename, d, stats);
                }
                else if (this.param.getBuffer_size() == 0) {
                    LOG.info("Classifying per file with native hashmap single");
                    this.classify_java_hashmap_single(filename, d, stats);
                }
                break;
            case HASHMULTIMAP_GUAVA:
                if (this.param.getBuffer_size() > 0) {
                    LOG.info("Classifying per file with Guava hashmultimap buffered");
                    this.classify_guava_hashmap_buffered(filename, d, stats);
                }
                else if (this.param.getBuffer_size() == 0) {
                    LOG.info("Classifying per file with Guava hashmultimap single");
                    this.classify_guava_hashmap_single(filename, d, stats);
                }
                break;
            default:
                //this.classify(filename, d, stats);
                break;

        }

    }
*/


    public void classify(String filename, BufferedWriter d, ClassificationStatistics stats) {

        LOG.warn("Entering classify");
        long initTime = System.nanoTime();

        try {

            long totalReads = 0;

            if (FilesysUtility.isFastaFile(filename)) {
                totalReads = FilesysUtility.readsInFastaFile(filename);

                LOG.warn("Number of reads in " + filename + " is " + totalReads + ".");


            }
            else if (FilesysUtility.isFastqFile(filename)) {
                totalReads = FilesysUtility.readsInFastqFile(filename);

                LOG.warn("Number of reads in " + filename + " is " + totalReads + ".");

            }
            else {
                LOG.error("Not recognized file format in " + filename );
                System.exit(1);
            }


            long startRead;
            int bufferSize = this.param.getBuffer_size();

            SequenceFileReaderLocal seqReader = new SequenceFileReaderLocal(filename, 0);

            LOG.info("Sequence reader created. Current index: " + seqReader.getReadedValues());

            SequenceData data;

            for(startRead = 0; startRead < totalReads; startRead+=bufferSize) {
                //while((currentRead < startRead+bufferSize) && ) {

                LOG.warn("Parsing new reads block. Starting in: "+startRead + " and ending in  " + (startRead + bufferSize));


                // Get corresponding hits for this buffer
                //List<List<MatchCandidate>> hits = this.db.accumulate_matches_native_buffered_best(f1, f2,
                //        startRead, bufferSize);
                Map<Long, List<MatchCandidate>> hits = this.db.accumulate_matches_single(filename,
                        startRead, bufferSize);

                LOG.warn("Results in buffer: " + hits.size() + ". Buffer size is:: "+bufferSize);

                //for(long i = 0;  (i < totalReads) && (i < currentRead + bufferSize); i++) {

                //LocationBasic current_key;

                long current_read;

                for(long i = 0;  i < hits.size() ; i++) {

                    current_read = startRead + i;
                    //Theoretically, number of sequences in data is the same as number of hits
                    data = seqReader.next();

                    if((i == 0) || (i == hits.size()-1)) {
                        LOG.warn("Read " + i + " is " + data.getHeader() + " :: " + data.getData());
                    }

                    if(data == null) {
                        LOG.warn("Data is null!! for hits: " + i + " and read " + (startRead + i));
                        break;
                    }

                    List<MatchCandidate> currentHits = hits.get(current_read);

                    this.process_database_answer(data.getHeader(), data.getData(),
                            "", currentHits, d, stats);

                }


            }

            seqReader.close();

            long endTime = System.nanoTime();

            LOG.warn("Time in classify_pairs_best is: " + ((endTime - initTime) / 1e9) + " seconds");
            //LOG.warn("Total characters readed: " + seqReader.getReadedValues());

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_pairs: "+e.getMessage());
            System.exit(1);
        }


    }


    /**
     * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
     * @param pathToFile The path to the FASTQ file
     * @return A JavaPairRDD containing <Long Read ID, String Read>
     */
    public JavaPairRDD<String, String> loadSequencesFromFile(String pathToFile) {
        JavaPairRDD<String,String> reads;

        //if (pathToFile.endsWith(".fastq") || pathToFile.endsWith(".fq") || pathToFile.endsWith(".fnq")) {
            reads = this.jsc.newAPIHadoopFile(pathToFile, FastqInputFormat.class, String.class, String.class, this.jsc.hadoopConfiguration());
            this.inputFormat = EnumModes.InputFormat.FASTQ;
        //}
        //else {
        //    reads = this.jsc.newAPIHadoopFile(pathToFile, FastaInputFormat.class, String.class, String.class, this.jsc.hadoopConfiguration());
        //    this.inputFormat = EnumModes.InputFormat.FASTA;
        //}

        return reads;

    }

    /*****************************************************************************
     *
     * @brief removes hits of a taxon on a certain rank; can be very slow!
     *
     *****************************************************************************/
    void remove_hits_on_rank(Taxonomy.Rank rank, long taxid) {
        HashMap<Location, Integer> maskedRes = (HashMap<Location, Integer>)this.hits.clone();


        for(Location currentLocation: this.hits.keySet()) {

            long r = this.db.ranks_of_target(currentLocation.getTargetId())[rank.ordinal()];
            // Todo: This function actually works????

            if(r != taxid) {
                //maskedRes.insert(this.hits.get(currentLocation));
            }
        }

        //using std::swap;
        //swap(maskedRes,this.hits);
    }



    public void process_database_answer(String header, String query1, String query2, TreeMap<LocationBasic, Integer> hits,
                                        BufferedWriter d, ClassificationStatistics stats) {
            /*
     const database& db, const query_param& param,
     const std::string& header,
     const sequence& query1, const sequence& query2,
     match_result&& hits, std::ostream& os, classification_statistics& stats)
     */

        if(header.isEmpty()) return;

        //preparation -------------------------------
        Classification groundTruth = new Classification();

        if(this.param.getProperties().isTestPrecision() ||
                (this.param.getProperties().getMapViewMode() != EnumModes.map_view_mode.none && this.param.getProperties().isShowGroundTruth()) ||
                (this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none) ) {

            groundTruth = this.db.ground_truth(header);

        }

        //clade exclusion
        if(this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
            long exclTaxid = this.db.ranks(groundTruth.tax())[this.param.getProperties().getExcludedRank().ordinal()];
            remove_hits_on_rank( this.param.getProperties().getExcludedRank(), exclTaxid); //Todo: Look what this function does
        }

        //classify ----------------------------------
        long numWindows = ( 2 + Math.max(query1.length() + query2.length(),this.param.getProperties().getInsertSizeMax()) / this.db.getTargetWindowStride_());

        MatchesInWindow tophits = new MatchesInWindow(hits, numWindows);
        Classification cls = this.sequence_classification(tophits);

        if(this.param.getProperties().isTestPrecision()) {
            Taxonomy.Rank lowestCorrectRank = this.db.lowest_common_rank( cls, groundTruth);

            stats.assign_known_correct(cls.rank(), groundTruth.rank(), lowestCorrectRank);

            //check if taxa of assigned target are covered
            if(param.getProperties().isTestCoverage() && groundTruth.has_taxon()) {
                update_coverage_statistics(cls, groundTruth, stats);
            }
        }
        else {
            //LOG.warn("[JMAbuin] Enter into assign with rank: " + Taxonomy.rank_name(cls.rank()));
            stats.assign(cls.rank());
        }

        boolean showMapping = (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.all) ||
                (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.mapped_only && !cls.none());

        try{
            if(showMapping) {
                //print query header and ground truth
                //show first contiguous string only
                int l = header.indexOf(' ');

                if (l != -1) {
                    d.write(header, 0, l);
                    /*
                    auto oit = std::ostream_iterator<char>{os, ""};
                    std::copy(header.begin(), header.begin() + l, oit);
                     */
                }
                else {
                    d.write(header);

                }

                d.write(this.param.getProperties().getOutSeparator());

                if(this.param.getProperties().isShowTopHits() || this.param.getProperties().isShowAllHits()) {

                    if(this.param.getProperties().isShowGroundTruth()) {
                        if(groundTruth.sequence_level()) {
                            this.show_ranks_of_target(d, this.db, groundTruth.target(),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else if(groundTruth.has_taxon()) {
                            this.show_ranks(d, this.db, this.db.ranks(groundTruth.tax()),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else {
                            d.write("n/a");
                        }

                        d.write(this.param.getProperties().getOutSeparator());
                    }
                }

                //print results
                if(this.param.getProperties().isShowAllHits()) {
                    show_matches(d, this.db, hits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if(this.param.getProperties().isShowTopHits()) {
                    show_matches(d, this.db, tophits, this.param.getProperties().getLowestRank());
                    d.write(param.getProperties().getOutSeparator());
                }
                if(this.param.getProperties().isShowLocations()) {
                    show_candidate_ranges(d, this.db, tophits);
                    d.write(this.param.getProperties().getOutSeparator());
                }
                show_classification(d, this.db, cls);

            }

            // BUSCA //HERE CHEMA mais abaixo
            if(this.param.getProperties().isTestAlignment() && !cls.none()) {
                SequenceOrigin origin = this.db.origin_of_target(tophits.target_id(0));

                SequenceFileReader reader = new SequenceFileReader(origin.getFilename(), 0);

                for(int i = 0; i < origin.getIndex(); ++i) {
                    reader.next();
                }

                SequenceData finalSeq = reader.next();

                if(finalSeq != null) {
                    String tgtSequ = finalSeq.getData();

                }

            }

        }
        catch(IOException e) {
            LOG.error("IOException in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }

/*

//HERE CHEMA
        //optional alignment ------------------------------
        if(param.testAlignment && !cls.none()) {
            //check which of the top sequences has a better alignment
        const auto& origin = db.origin_of_target(tophits.target_id(0));

            try {
                //load candidate file and forward to sequence
                auto reader = make_sequence_reader(origin.filename);
                for(std::size_t i = 0; i < origin.index; ++i) reader->next();
                if(reader->has_next()) {
                    auto tgtSequ = reader->next().data;
                    auto subject = make_view_from_window_range(tgtSequ,
                            tophits.window(0),
                            db.target_window_stride());

                    auto align = make_alignment(query1, query2, subject,
                            needleman_wunsch_scheme{});

                    stats.register_alignment_score(align.score);

                    //print alignment to top candidate
                    if(align.score > 0 && param.showAlignment) {
                    const auto w = db.target_window_stride();

                        os << '\n'
                                << param.comment << "  score  " << align.score
                                << "  aligned to "
                                << origin.filename << " #" << origin.index
                                << " in range [" << (w * tophits.window(0).beg)
                                << ',' << (w * tophits.window(0).end) << "]\n"
                                << param.comment << "  query  " << align.query << '\n'
                                << param.comment << "  target " << align.subject;
                    }
                }
            } catch(std::exception&) { }
        }

        if(showMapping) os << '\n';
        */
    }

    public void process_database_answer_basic(String header, String query1, String query2, List<LocationBasic> hits,
                                        BufferedWriter d, ClassificationStatistics stats) {
        /*
     const database& db, const query_param& param,
     const std::string& header,
     const sequence& query1, const sequence& query2,
     match_result&& hits, std::ostream& os, classification_statistics& stats)
     */

        if(header.isEmpty()) return;

        //preparation -------------------------------
        Classification groundTruth = new Classification();

        if(this.param.getProperties().isTestPrecision() ||
                (this.param.getProperties().getMapViewMode() != EnumModes.map_view_mode.none && this.param.getProperties().isShowGroundTruth()) ||
                (this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none) ) {

            groundTruth = this.db.ground_truth(header);

        }

        //clade exclusion
        if(this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
            long exclTaxid = this.db.ranks(groundTruth.tax())[this.param.getProperties().getExcludedRank().ordinal()];
            remove_hits_on_rank( this.param.getProperties().getExcludedRank(), exclTaxid); //Todo: Look what this function does
        }

        //classify ----------------------------------
        long numWindows = ( 2 + Math.max(query1.length() + query2.length(),this.param.getProperties().getInsertSizeMax()) / this.db.getTargetWindowStride_());

        //LOG.warn("Max Number of Windows: " + numWindows);
        //LOG.warn("Size 1: " + query1.length());
        //LOG.warn("Size 2: " + query2.length());
        //LOG.warn("Insert max: " + this.param.getProperties().getInsertSizeMax());
        //LOG.warn("Stride: " + this.db.getTargetWindowStride_());
        MatchesInWindowBasic matches = new MatchesInWindowBasic(hits, numWindows, this.param, this.db.getTaxa_(), this.db.getTargets_());
        //matches.print_all_hits();
        Classification cls = this.sequence_classification(matches);
        //cls.print();

        if(this.param.getProperties().isTestPrecision()) {
            //LOG.warn("[JMAbuin] Enter into assign precision with rank: " + Taxonomy.rank_name(cls.rank()));
            Taxonomy.Rank lowestCorrectRank = this.db.lowest_common_rank( cls, groundTruth);

            //LOG.warn("Classification: " + cls.rank().name());
            //LOG.warn(this.db.getTargets_().get((int)cls.target()).getIdentifier());
            //LOG.warn(this.db.getTargets_().get((int)cls.target()).getOrigin().getFilename());
            //LOG.warn("Groundtruth: " + groundTruth.rank().name());
            //LOG.warn("Lowest correct rank: " + lowestCorrectRank.name());


            stats.assign_known_correct(cls.rank(), groundTruth.rank(), lowestCorrectRank);

            //check if taxa of assigned target are covered
            if(this.param.getProperties().isTestCoverage() && groundTruth.has_taxon()) {
                update_coverage_statistics(cls, groundTruth, stats);
            }
        }
        else {
            //LOG.warn("[JMAbuin] Enter into assign with rank: " + Taxonomy.rank_name(cls.rank()));
            stats.assign(cls.rank());
        }

        boolean showMapping = (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.all) ||
                (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.mapped_only && !cls.none());

        try{
            if(showMapping) {
                //print query header and ground truth
                //show first contiguous string only
                int l = header.indexOf(' ');

                if (l != -1) {
                    d.write(header, 0, l);
                    /*
                    auto oit = std::ostream_iterator<char>{os, ""};
                    std::copy(header.begin(), header.begin() + l, oit);
                     */
                }
                else {
                    d.write(header);

                }

                d.write(this.param.getProperties().getOutSeparator());

                if(this.param.getProperties().isShowTopHits() || this.param.getProperties().isShowAllHits()) {

                    if(this.param.getProperties().isShowGroundTruth()) {
                        if(groundTruth.sequence_level()) {
                            show_ranks_of_target(d, this.db, groundTruth.target(),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else if(groundTruth.has_taxon()) {
                            show_ranks(d, this.db, this.db.ranks(groundTruth.tax()),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else {
                            d.write("n/a");
                        }

                        d.write(this.param.getProperties().getOutSeparator());
                    }
                }

                //print results
                if (this.param.getProperties().isShowAllHits()) {
                    //LOG.warn("Showing all hits");
                    show_matches_basic(d, this.db, matches.get_all_hits(), this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if (this.param.getProperties().isShowTopHits()) {
                    //LOG.warn("Showing top hits");
                    show_matches_basic(d, this.db, matches.get_top_hits(), this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if (this.param.getProperties().isShowLocations()) {
                    show_candidate_ranges(d, this.db, matches.getMatches());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                show_classification(d, this.db, cls);

            }

            // BUSCA //HERE CHEMA mais abaixo
            /*if(this.param.getProperties().isTestAlignment() && !cls.none()) {
                SequenceOrigin origin = this.db.origin_of_target(tophits.target_id(0));

                SequenceFileReader reader = new SequenceFileReader(origin.getFilename(), 0);

                for(int i = 0; i < origin.getIndex(); ++i) {
                    reader.next();
                }

                SequenceData finalSeq = reader.next();

                if(finalSeq != null) {
                    String tgtSequ = finalSeq.getData();

                }

            }*/

        }
        catch(IOException e) {
            LOG.error("IOException in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
    }
/*
    public void process_database_answer(String header, String query1, String query2, List<LocationBasic> hits,
                                              BufferedWriter d, ClassificationStatistics stats) {

        if(header.isEmpty()) return;

        //preparation -------------------------------
        Classification groundTruth = new Classification();

        if(this.param.getProperties().isTestPrecision() ||
                (this.param.getProperties().getMapViewMode() != EnumModes.map_view_mode.none && this.param.getProperties().isShowGroundTruth()) ||
                (this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none) ) {

            groundTruth = this.db.ground_truth(header);

        }

        //clade exclusion
        if(this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
            long exclTaxid = this.db.ranks(groundTruth.tax())[this.param.getProperties().getExcludedRank().ordinal()];
            remove_hits_on_rank( this.param.getProperties().getExcludedRank(), exclTaxid); //Todo: Look what this function does
        }

        //classify ----------------------------------
        long numWindows = ( 2 + Math.max(query1.length() + query2.length(),this.param.getProperties().getInsertSizeMax()) / this.db.getTargetWindowStride_());

        //LOG.warn("Starting classification");
        MatchesInWindowList tophits = new MatchesInWindowList(hits, (int)numWindows, this.db.getTargets_(), this.db.getTaxa_(), this.param);
        Classification cls = this.sequence_classification(tophits);
        //LOG.warn("Starting classification done");
        if(this.param.getProperties().isTestPrecision()) {
            //LOG.warn("[JMAbuin] Enter into assign precision with rank: " + Taxonomy.rank_name(cls.rank()));
            Taxonomy.Rank lowestCorrectRank = this.db.lowest_common_rank( cls, groundTruth);

            //LOG.warn("Classification: " + cls.rank().name());
            //LOG.warn(this.db.getTargets_().get((int)cls.target()).getIdentifier());
            //LOG.warn(this.db.getTargets_().get((int)cls.target()).getOrigin().getFilename());
            //LOG.warn("Groundtruth: " + groundTruth.rank().name());
            //LOG.warn("Lowest correct rank: " + lowestCorrectRank.name());


            stats.assign_known_correct(cls.rank(), groundTruth.rank(), lowestCorrectRank);

            //check if taxa of assigned target are covered
            if(this.param.getProperties().isTestCoverage() && groundTruth.has_taxon()) {
                update_coverage_statistics(cls, groundTruth, stats);
            }
        }
        else {
            //LOG.warn("[JMAbuin] Enter into assign with rank: " + Taxonomy.rank_name(cls.rank()));
            stats.assign(cls.rank());
        }

        boolean showMapping = (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.all) ||
                (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.mapped_only && !cls.none());

        try{
            if(showMapping) {
                //print query header and ground truth
                //show first contiguous string only
                int l = header.indexOf(' ');

                if (l != -1) {
                    d.write(header, 0, l);

                }
                else {
                    d.write(header);

                }

                d.write(this.param.getProperties().getOutSeparator());

                if(this.param.getProperties().isShowTopHits() || this.param.getProperties().isShowAllHits()) {

                    if(this.param.getProperties().isShowGroundTruth()) {
                        if(groundTruth.sequence_level()) {
                            show_ranks_of_target(d, this.db, groundTruth.target(),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else if(groundTruth.has_taxon()) {
                            show_ranks(d, this.db, this.db.ranks(groundTruth.tax()),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else {
                            d.write("n/a");
                        }

                        d.write(this.param.getProperties().getOutSeparator());
                    }
                }

                //print results
                if (this.param.getProperties().isShowAllHits()) {
                    //LOG.warn("Showing all hits");
                    show_matches_basic(d, this.db, hits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if (this.param.getProperties().isShowTopHits()) {
                    //LOG.warn("Showing top hits");
                    show_matches_basic(d, this.db, tophits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if (this.param.getProperties().isShowLocations()) {
                    show_candidate_ranges(d, this.db, tophits);
                    d.write(this.param.getProperties().getOutSeparator());
                }
                show_classification(d, this.db, cls);

            }



        }
        catch(IOException e) {
            LOG.error("IOException in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
    }
*/
    public void process_database_answer(String header, String query1, String query2, List<MatchCandidate> hits,
                                        BufferedWriter d, ClassificationStatistics stats) {
        /*
     const database& db, const query_param& param,
     const std::string& header,
     const sequence& query1, const sequence& query2,
     match_result&& hits, std::ostream& os, classification_statistics& stats)
     */

        if(header.isEmpty()) return;

        //preparation -------------------------------
        Classification groundTruth = new Classification();

        if(this.param.getProperties().isTestPrecision() ||
                (this.param.getProperties().getMapViewMode() != EnumModes.map_view_mode.none && this.param.getProperties().isShowGroundTruth()) ||
                (this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none) ) {

            groundTruth = this.db.ground_truth(header);

        }

        //clade exclusion
        if(this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
            long exclTaxid = this.db.ranks(groundTruth.tax())[this.param.getProperties().getExcludedRank().ordinal()];
            remove_hits_on_rank( this.param.getProperties().getExcludedRank(), exclTaxid); //Todo: Look what this function does
        }

        //classify ----------------------------------
        long numWindows = ( 2 + Math.max(query1.length() + query2.length(),this.param.getProperties().getInsertSizeMax()) / this.db.getTargetWindowStride_());

        //LOG.warn("Starting classification");
        MatchesInWindowList tophits = new MatchesInWindowList(hits, (int)numWindows, this.db.getTargets_(), this.db.getTaxa_(), this.param);
        //tophits.print_top_hits();
        Classification cls = this.sequence_classification(tophits);
        //cls.print();
        //LOG.warn("Starting classification done");
        if(this.param.getProperties().isTestPrecision()) {
            //LOG.warn("[JMAbuin] Enter into assign precision with rank: " + Taxonomy.rank_name(cls.rank()));
            Taxonomy.Rank lowestCorrectRank = this.db.lowest_common_rank( cls, groundTruth);

            //LOG.warn("Classification: " + cls.rank().name());
            //LOG.warn(this.db.getTargets_().get((int)cls.target()).getIdentifier());
            //LOG.warn(this.db.getTargets_().get((int)cls.target()).getOrigin().getFilename());
            //LOG.warn("Groundtruth: " + groundTruth.rank().name());
            //LOG.warn("Lowest correct rank: " + lowestCorrectRank.name());


            stats.assign_known_correct(cls.rank(), groundTruth.rank(), lowestCorrectRank);

            //check if taxa of assigned target are covered
            if(this.param.getProperties().isTestCoverage() && groundTruth.has_taxon()) {
                update_coverage_statistics(cls, groundTruth, stats);
            }
        }
        else {
            //LOG.warn("[JMAbuin] Enter into assign with rank: " + Taxonomy.rank_name(cls.rank()));
            stats.assign(cls.rank());
        }

        boolean showMapping = (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.all) ||
                (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.mapped_only && !cls.none());

        try{
            if(showMapping) {
                //print query header and ground truth
                //show first contiguous string only
                int l = header.indexOf(' ');

                if (l != -1) {
                    d.write(header, 0, l);
                    /*
                    auto oit = std::ostream_iterator<char>{os, ""};
                    std::copy(header.begin(), header.begin() + l, oit);
                     */
                }
                else {
                    d.write(header);

                }

                d.write(this.param.getProperties().getOutSeparator());

                if(this.param.getProperties().isShowTopHits() || this.param.getProperties().isShowAllHits()) {

                    if(this.param.getProperties().isShowGroundTruth()) {
                        if(groundTruth.sequence_level()) {
                            show_ranks_of_target(d, this.db, groundTruth.target(),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else if(groundTruth.has_taxon()) {
                            show_ranks(d, this.db, this.db.ranks(groundTruth.tax()),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else {
                            d.write("n/a");
                        }

                        d.write(this.param.getProperties().getOutSeparator());
                    }
                }

                //print results
                if (this.param.getProperties().isShowAllHits()) {
                    //LOG.warn("Showing all hits");
                    show_matches_list(d, this.db, tophits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if (this.param.getProperties().isShowTopHits()) {
                    //LOG.warn("Showing top hits");
                    show_matches_list(d, this.db, tophits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                /*if (this.param.getProperties().isShowLocations()) {
                    show_candidate_ranges(d, this.db, tophits);
                    d.write(this.param.getProperties().getOutSeparator());
                }*/
                show_classification(d, this.db, cls);

            }



        }
        catch(IOException e) {
            LOG.error("IOException in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
    }



    public void process_database_answer_native(String header, String query1, String query2, HashMap<LocationBasic, Integer> hits,
                                              BufferedWriter d, ClassificationStatistics stats) {
        /*
     const database& db, const query_param& param,
     const std::string& header,
     const sequence& query1, const sequence& query2,
     match_result&& hits, std::ostream& os, classification_statistics& stats)
     */

        if(header.isEmpty()) return;

        //preparation -------------------------------
        Classification groundTruth = new Classification();

        if(this.param.getProperties().isTestPrecision() ||
                (this.param.getProperties().getMapViewMode() != EnumModes.map_view_mode.none && this.param.getProperties().isShowGroundTruth()) ||
                (this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none) ) {

            groundTruth = this.db.ground_truth(header);

        }

        //clade exclusion
        if(this.param.getProperties().getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
            long exclTaxid = this.db.ranks(groundTruth.tax())[this.param.getProperties().getExcludedRank().ordinal()];
            remove_hits_on_rank( this.param.getProperties().getExcludedRank(), exclTaxid); //Todo: Look what this function does
        }

        //classify ----------------------------------
        long numWindows = ( 2 + Math.max(query1.length() + query2.length(),this.param.getProperties().getInsertSizeMax()) / this.db.getTargetWindowStride_());

        MatchesInWindowNative tophits = new MatchesInWindowNative(hits, numWindows);
        Classification cls = this.sequence_classification(tophits);

        if(this.param.getProperties().isTestPrecision()) {
            Taxonomy.Rank lowestCorrectRank = db.lowest_common_rank( cls, groundTruth);

            stats.assign_known_correct(cls.rank(), groundTruth.rank(), lowestCorrectRank);

            //check if taxa of assigned target are covered
            if(this.param.getProperties().isTestCoverage() && groundTruth.has_taxon()) {
                update_coverage_statistics(cls, groundTruth, stats);
            }
        }
        else {
            //LOG.warn("[JMAbuin] Enter into assign with rank: " + Taxonomy.rank_name(cls.rank()));
            stats.assign(cls.rank());
        }

        boolean showMapping = (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.all) ||
                (this.param.getProperties().getMapViewMode() == EnumModes.map_view_mode.mapped_only && !cls.none());

        try{
            if(showMapping) {
                //print query header and ground truth
                //show first contiguous string only
                int l = header.indexOf(' ');

                if (l != -1) {
                    d.write(header, 0, l);
                    /*
                    auto oit = std::ostream_iterator<char>{os, ""};
                    std::copy(header.begin(), header.begin() + l, oit);
                     */
                }
                else {
                    d.write(header);

                }

                d.write(this.param.getProperties().getOutSeparator());

                if(this.param.getProperties().isShowTopHits() || this.param.getProperties().isShowAllHits()) {

                    if(this.param.getProperties().isShowGroundTruth()) {
                        if(groundTruth.sequence_level()) {
                            show_ranks_of_target(d, this.db, groundTruth.target(),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else if(groundTruth.has_taxon()) {
                            show_ranks(d, this.db, this.db.ranks(groundTruth.tax()),
                                    this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(),
                                    this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank());
                        }
                        else {
                            d.write("n/a");
                        }

                        d.write(this.param.getProperties().getOutSeparator());
                    }
                }

                //print results
                if(this.param.getProperties().isShowAllHits()) {
                    show_matches_native(d, this.db, hits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if(this.param.getProperties().isShowTopHits()) {
                    show_matches_native(d, this.db, tophits, this.param.getProperties().getLowestRank());
                    d.write(this.param.getProperties().getOutSeparator());
                }
                if(this.param.getProperties().isShowLocations()) {
                    show_candidate_ranges(d, this.db, tophits);
                    d.write(this.param.getProperties().getOutSeparator());
                }
                show_classification(d, this.db, cls);

            }

            // BUSCA //HERE CHEMA mais abaixo
            if(this.param.getProperties().isTestAlignment() && !cls.none()) {
                SequenceOrigin origin = this.db.origin_of_target(tophits.target_id(0));

                SequenceFileReader reader = new SequenceFileReader(origin.getFilename(), 0);

                for(int i = 0; i < origin.getIndex(); ++i) {
                    reader.next();
                }

                SequenceData finalSeq = reader.next();

                if(finalSeq != null) {
                    String tgtSequ = finalSeq.getData();

                }

            }

        }
        catch(IOException e) {
            LOG.error("IOException in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function process_database_answer: "+ e.getMessage());
            System.exit(1);
        }
    }


    public void show_ranks_of_target(BufferedWriter os, Database db, long tid, EnumModes.taxon_print_mode mode, Taxonomy.Rank lowest,
                                     Taxonomy.Rank highest) {
        //since targets don't have their own taxonId, print their sequence id
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                if(mode != EnumModes.taxon_print_mode.id_only) {
                    os.write("sequence:"+db.sequence_id_of_target(tid));
                    os.newLine();

                } else {
                    os.write(db.sequence_id_of_target(tid));
                    os.newLine();
                }
            }

            if(highest == Taxonomy.Rank.Sequence) return;

            if(lowest == Taxonomy.Rank.Sequence) os.write(',');

            show_ranks(os, db, db.ranks_of_target((int)tid), mode, lowest, highest);
        }
        catch(IOException e) {
            LOG.error("IOException in function show_ranks_of_target: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_ranks_of_target: "+ e.getMessage());
            System.exit(1);
        }

    }

    public void show_ranks_of_target(BufferedWriter os, Database db, Taxon tid, EnumModes.taxon_print_mode mode, Taxonomy.Rank lowest,
                                     Taxonomy.Rank highest) {
        //since targets don't have their own taxonId, print their sequence id
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                if(mode != EnumModes.taxon_print_mode.id_only) {
                    os.write("sequence:"+tid.getTaxonName());
                    os.newLine();

                } //else {
                //    os.write(db.sequence_id_of_target(tid));
                //    os.newLine();
                //}
            }

            if(highest == Taxonomy.Rank.Sequence) return;

            if(lowest == Taxonomy.Rank.Sequence) os.write(',');

            //show_ranks(os, db, db.ranks_of_target((int)tid), mode, lowest, highest);
            //LOG.warn("The taxon id in show_ranks_of_target is: " + tid.getTaxonId());
            show_ranks(os, db, this.db.getTaxa_().ranks((Long)tid.getTaxonId()), mode, lowest, highest);
        }
        catch(IOException e) {
            LOG.error("IOException in function show_ranks_of_target: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_ranks_of_target: "+ e.getMessage());
            System.exit(1);
        }

    }


    public Classification sequence_classification(MatchesInWindow cand) {

        long wc = this.param.getProperties().isWeightHitsWithWindows() ? cand.covered_windows() > 1 ? cand.covered_windows() - 1 : 1 : 1;

        //sum of top-2 hits < threshold => considered not classifiable
        if((cand.hits(0) + cand.hits(1)) < wc * this.param.getProperties().getHitsMin()) {
            //LOG.warn("[JMAbuin] First if");
            return new Classification();
        }

        //either top 2 are the same sequences with at least 'hitsMin' many hits
        //(checked before) or hit difference between these top 2 is above threshhold
        if( (cand.target_id(0) == cand.target_id(1))
                || (cand.hits(0) - cand.hits(1)) >= wc * this.param.getProperties().getHitsMin())
        {
            //return top candidate
            int tid = cand.target_id(0);
            //LOG.warn("[JMAbuin] Second if with tid: "+tid);
            return new Classification(tid, db.taxon_of_target((long)tid));
        }

        //LOG.warn("[JMAbuin] Last if");
        return new Classification(this.lowest_common_taxon(MatchesInWindow.maxNo, cand, (float)this.param.getProperties().getHitsDiff(),
                this.param.getProperties().getLowestRank(), this.param.getProperties().getHighestRank()));


    }

    public Classification sequence_classification(MatchesInWindowBasic cand) {


        if (cand.get_top_hits().isEmpty()) {
            //LOG.warn("Top list is empty!");
            return new Classification();
        }

        //int best = cand.get_top_hits().get(0).getTgt();
        //return new Classification(best, lowest_common_taxon(MatchesInWindowNative.maxNo, cand, (float) this.param.getProperties().getHitsDiff(),
        //        this.param.getProperties().getLowestRank(), this.param.getProperties().getHighestRank()));
        return new Classification(lowest_common_taxon(MatchesInWindowNative.maxNo, cand, (float) this.param.getProperties().getHitsDiff(),
                        this.param.getProperties().getLowestRank(), this.param.getProperties().getHighestRank()));

    }

    public Classification sequence_classification(MatchesInWindowList cand) {


        if (cand.getTop_list().isEmpty()) {
            //LOG.warn("Top list is empty!");
            return new Classification();
        }

        //int best = cand.get_top_hits().get(0).getTgt();
        //return new Classification(best, lowest_common_taxon(MatchesInWindowNative.maxNo, cand, (float) this.param.getProperties().getHitsDiff(),
        //        this.param.getProperties().getLowestRank(), this.param.getProperties().getHighestRank()));
        return new Classification(lowest_common_taxon(MatchesInWindowNative.maxNo, cand, (float) this.param.getProperties().getHitsDiff(),
                this.param.getProperties().getLowestRank(), this.param.getProperties().getHighestRank()));



    }


    public Classification sequence_classification(MatchesInWindowNative cand) {

        long wc = this.param.getProperties().isWeightHitsWithWindows() ? cand.covered_windows() > 1 ? cand.covered_windows() - 1 : 1 : 1;

        //sum of top-2 hits < threshold => considered not classifiable
        if((cand.hits(0) + cand.hits(1)) < wc * this.param.getProperties().getHitsMin()) {
            //LOG.warn("[JMAbuin] First if");
            return new Classification();
        }

        //either top 2 are the same sequences with at least 'hitsMin' many hits
        //(checked before) or hit difference between these top 2 is above threshhold
        if( (cand.target_id(0) == cand.target_id(1))
                || (cand.hits(0) - cand.hits(1)) >= wc * this.param.getProperties().getHitsMin())
        {
            //return top candidate
            int tid = cand.target_id(0);
            //LOG.warn("[JMAbuin] Second if with tid: "+tid);
            return new Classification(tid, db.taxon_of_target((long)tid));
        }

        //LOG.warn("[JMAbuin] Last if");
        return new Classification(lowest_common_taxon(MatchesInWindowNative.maxNo, cand, (float) this.param.getProperties().getHitsDiff(),
                this.param.getProperties().getLowestRank(), this.param.getProperties().getHighestRank()));


    }


    public Taxon lowest_common_taxon(int maxn, MatchesInWindow cand,
                                     float trustedMajority, Taxonomy.Rank lowestRank,
                                     Taxonomy.Rank highestRank) {

        if(lowestRank == null) {
            lowestRank = Taxonomy.Rank.subSpecies;
        }

        if(highestRank == null) {
            highestRank = Taxonomy.Rank.Domain;
        }

        if(maxn < 3 || cand.count() < 3) {
        //if(cand.count() < 3) {

            //LOG.warn("[JMABUIN] Un: " + cand.target_id(0) + ", dous: " + cand.target_id(1));

            Taxon tax = db.ranked_lca_of_targets(cand.target_id(0), cand.target_id(1));

            //classify if rank is below or at the highest rank of interest
            if (tax.getRank().ordinal() <= highestRank.ordinal()) {
                //LOG.warn("[JMABUIN] returning tax: " + cand.target_id(0) + ", dous: " + cand.target_id(1));
                //LOG.warn("[JMABUIN] ranks: " + tax.getRank().ordinal() + ", dous: " +highestRank.ordinal());
                return tax;
            }
            //else {
            //	LOG.warn("[JMABUIN] NOT returning tax: " + cand.target_id(0) + ", dous: " + cand.target_id(1));
            //	LOG.warn("[JMABUIN] ranks: " + tax.getRank().ordinal() + ", dous: " +highestRank.ordinal());
            //}
        }
        else{

            if(lowestRank == Taxonomy.Rank.Sequence) lowestRank = lowestRank.next();

            //LOG.warn("[JMABUIN] Indo polo else: " + cand.target_id(0) + ", dous: " + cand.target_id(1));
            HashMap<Long, Long> scores = new HashMap<Long, Long>(2*cand.count());

            for(Taxonomy.Rank r = lowestRank; r.ordinal() <= highestRank.ordinal(); r = Taxonomy.next_main_rank(r)) {

                //hash-count taxon id occurrences on rank 'r'
                int totalscore = 0;
                for(int i = 0, n = cand.count(); i < n; ++i) {
                    //use target id instead of taxon if at sequence level
                    long taxid;

                    if((db.ranks_of_target_basic(cand.target_id(i)) == null) || (db.ranks_of_target_basic(cand.target_id(i)).length == 0)) {
                        taxid = 0;
                    }
                    else {
                        taxid = db.ranks_of_target_basic(cand.target_id(i))[r.ordinal()];
                    }

                    if(taxid > 0) {
                        long score = cand.hits(i);
                        totalscore += score;

                        Long it = scores.get(taxid);

                        if(it != null) {
                            scores.put(taxid, scores.get(taxid) + score);
                        }
                        else {
                            scores.put(taxid, score);
                        }

                    }
                }

//            std::cout << "\n    " << taxonomy::rank_name(r) << " ";

                //determine taxon id with most votes
                long toptid = 0;
                long topscore = 0;

                for(Map.Entry<Long, Long> x : scores.entrySet()) {

//                std::cout << x.first << ":" << x.second << ", ";

                    if(x.getValue() > topscore) {
                        toptid = x.getKey();
                        topscore = x.getValue();
                    }
                }

                //if enough candidates (weighted by their hits)
                //agree on a taxon => classify as such
                if(topscore >= (totalscore * trustedMajority)) {

//                std::cout << "  => classified " << taxonomy::rank_name(r)
//                          << " " << toptid << '\n';

                    return db.taxon_with_id(toptid);
                }

                scores.clear();
            }


        }

        //candidates couldn't agree on a taxon on any relevant taxonomic rank
        return null;

    }

    public Taxon lowest_common_taxon(int maxn, MatchesInWindowBasic cand,
                                     float trustedMajority, Taxonomy.Rank lowestRank,
                                     Taxonomy.Rank highestRank) {

        if(lowestRank == null) {
            lowestRank = Taxonomy.Rank.subSpecies;
        }

        if(highestRank == null) {
            highestRank = Taxonomy.Rank.Domain;
        }

        //int best_pos = cand.getTop_list().size() - 1;
        Taxon lca = db.taxon_of_target((long)cand.get_top_hits().get(0).getTgt());
        //LOG.warn("Best taxon is: " + lca.getTaxonName() + ", hits: " + cand.get_top_hits().get(0).getHits());

        for (int i = 1; i < cand.get_top_hits().size(); ++i) {
            //LOG.warn("Other taxon is: " + db.taxon_of_target((long)cand.get_top_hits().get(i).getTgt()).getTaxonName() +
             //       ", hits: " + cand.get_top_hits().get(i).getHits());
            lca = this.db.ranked_lca(lca, cand.get_top_hits().get(i).getTax());
            //LOG.warn("Obtained LCA: " + lca.getTaxonName());
            if (lca == null || lca == this.db.getTaxa_().getNoTaxon_() || lca.getRank().ordinal() > this.param.getProperties().getHighestRank().ordinal()) {
                return this.db.getTaxa_().getNoTaxon_();
            }
            else {
                break;
            }


        }

        //LOG.warn("Final LCA is: " + lca.getTaxonName());

        return lca;

        /*
        if(lowestRank == null) {
            lowestRank = Taxonomy.Rank.subSpecies;
        }

        if(highestRank == null) {
            highestRank = Taxonomy.Rank.Domain;
        }

        if(maxn < 3 || cand.count() < 3) {

            //LOG.warn("[JMABUIN] Un: "+cand.target_id(0)+ ", dous: "+cand.target_id(1));

            //TODO: HERE
            Taxon tax = db.ranked_lca_of_targets(cand.target_id(0), cand.target_id(1));

            //classify if rank is below or at the highest rank of interest
            if(tax.getRank().ordinal() <= highestRank.ordinal()) {
                return tax;
            }

        }
        else{

            if(lowestRank == Taxonomy.Rank.Sequence) lowestRank = lowestRank.next();
            HashMap<Long, Long> scores = new HashMap<Long, Long>(2*cand.count());

            for(Taxonomy.Rank r = lowestRank; r.ordinal() <= highestRank.ordinal(); r = Taxonomy.next_main_rank(r)) {

                //hash-count taxon id occurrences on rank 'r'
                int totalscore = 0;
                for(int i = 0, n = cand.count(); i < n; ++i) {
                    //use target id instead of taxon if at sequence level
                    long taxid = db.ranks_of_target_basic(cand.target_id(i))[r.ordinal()];
                    if(taxid > 0) {
                        long score = cand.hits(i);
                        totalscore += score;

                        Long it = scores.get(taxid);

                        if(it != null) {
                            scores.put(taxid, scores.get(taxid) + score);
                        }
                        else {
                            scores.put(taxid, score);
                        }

                    }
                }

//            std::cout << "\n    " << taxonomy::rank_name(r) << " ";

                //determine taxon id with most votes
                long toptid = 0;
                long topscore = 0;

                for(Map.Entry<Long, Long> x : scores.entrySet()) {

//                std::cout << x.first << ":" << x.second << ", ";

                    if(x.getValue() > topscore) {
                        toptid = x.getKey();
                        topscore = x.getValue();
                    }
                }

                //if enough candidates (weighted by their hits)
                //agree on a taxon => classify as such
                if(topscore >= (totalscore * trustedMajority)) {

//                std::cout << "  => classified " << taxonomy::rank_name(r)
//                          << " " << toptid << '\n';

                    return db.taxon_with_id(toptid);
                }

                scores.clear();
            }


        }

        //candidates couldn't agree on a taxon on any relevant taxonomic rank
        return null;
*/
    }

    public Taxon lowest_common_taxon(int maxn, MatchesInWindowList cand,
                                     float trustedMajority, Taxonomy.Rank lowestRank,
                                     Taxonomy.Rank highestRank) {
        if(lowestRank == null) {
            lowestRank = Taxonomy.Rank.subSpecies;
        }

        if(highestRank == null) {
            highestRank = Taxonomy.Rank.Domain;
        }

        //int best_pos = cand.getTop_list().size() - 1;
        Taxon lca = db.taxon_of_target((long)cand.getTop_list().get(0).getTgt());
        //LOG.warn("Best taxon is: " + lca.getTaxonName() + ", hits: " + cand.getTop_list().get(0).getHits());

        for (int i = 1; i < cand.getTop_list().size(); ++i) {
            //LOG.warn("Other taxon is: " + db.taxon_of_target((long)cand.getTop_list().get(i).getTgt()).getTaxonName() +
            //       ", hits: " + cand.getTop_list().get(i).getHits());
            lca = this.db.ranked_lca(lca, cand.getTop_list().get(i).getTax());
            //LOG.warn("Obtained LCA: " + lca.getTaxonName());
            if (lca == null || lca == this.db.getTaxa_().getNoTaxon_() || lca.getRank().ordinal() > this.param.getProperties().getHighestRank().ordinal()) {
                return this.db.getTaxa_().getNoTaxon_();
            }
            else {
                break;
            }


        }

        //LOG.warn("Final LCA is: " + lca.getTaxonName());

        return lca;
        /*

        if(maxn < 3 || cand.count() < 3) {

            //LOG.warn("[JMABUIN] Un: "+cand.target_id(0)+ ", dous: "+cand.target_id(1));

            int best_pos = cand.getTop_list().size() - 1;
            int second_best_pos = cand.getTop_list().size() - 2;
            Taxon tax = db.ranked_lca_of_targets(cand.target_id(best_pos), cand.target_id(second_best_pos));

            //classify if rank is below or at the highest rank of interest
            if(tax.getRank().ordinal() <= highestRank.ordinal()) {
                return tax;
            }

        }
        else{

            if(lowestRank == Taxonomy.Rank.Sequence) lowestRank = lowestRank.next();
            HashMap<Long, Long> scores = new HashMap<Long, Long>(2*cand.count());

            for(Taxonomy.Rank r = lowestRank; r.ordinal() <= highestRank.ordinal(); r = Taxonomy.next_main_rank(r)) {

                //hash-count taxon id occurrences on rank 'r'
                int totalscore = 0;
                for(int i = 0, n = cand.count(); i < n; ++i) {
                    //use target id instead of taxon if at sequence level
                    long taxid = db.ranks_of_target_basic(cand.target_id(i))[r.ordinal()];
                    if(taxid > 0) {
                        long score = cand.hits(i);
                        totalscore += score;

                        Long it = scores.get(taxid);

                        if(it != null) {
                            scores.put(taxid, scores.get(taxid) + score);
                        }
                        else {
                            scores.put(taxid, score);
                        }

                    }
                }

//            std::cout << "\n    " << taxonomy::rank_name(r) << " ";

                //determine taxon id with most votes
                long toptid = 0;
                long topscore = 0;

                for(Map.Entry<Long, Long> x : scores.entrySet()) {

//                std::cout << x.first << ":" << x.second << ", ";

                    if(x.getValue() > topscore) {
                        toptid = x.getKey();
                        topscore = x.getValue();
                    }
                }

                //if enough candidates (weighted by their hits)
                //agree on a taxon => classify as such
                if(topscore >= (totalscore * trustedMajority)) {

//                std::cout << "  => classified " << taxonomy::rank_name(r)
//                          << " " << toptid << '\n';

                    return db.taxon_with_id(toptid);
                }

                scores.clear();
            }


        }

        //candidates couldn't agree on a taxon on any relevant taxonomic rank
        return null;
*/
    }

    public Taxon lowest_common_taxon(int maxn, MatchesInWindowNative cand,
                                     float trustedMajority, Taxonomy.Rank lowestRank,
                                     Taxonomy.Rank highestRank) {
        if(lowestRank == null) {
            lowestRank = Taxonomy.Rank.subSpecies;
        }

        if(highestRank == null) {
            highestRank = Taxonomy.Rank.Domain;
        }

        if(maxn < 3 || cand.count() < 3) {

            //LOG.warn("[JMABUIN] Un: "+cand.target_id(0)+ ", dous: "+cand.target_id(1));

            //TODO: HERE
            Taxon tax = db.ranked_lca_of_targets(cand.target_id(0), cand.target_id(1));

            //classify if rank is below or at the highest rank of interest
            if(tax.getRank().ordinal() <= highestRank.ordinal()) {
                return tax;
            }

        }
        else{

            if(lowestRank == Taxonomy.Rank.Sequence) lowestRank = lowestRank.next();
            HashMap<Long, Long> scores = new HashMap<Long, Long>(2*cand.count());

            for(Taxonomy.Rank r = lowestRank; r.ordinal() <= highestRank.ordinal(); r = Taxonomy.next_main_rank(r)) {

                //hash-count taxon id occurrences on rank 'r'
                int totalscore = 0;
                for(int i = 0, n = cand.count(); i < n; ++i) {
                    //use target id instead of taxon if at sequence level
                    long taxid = db.ranks_of_target_basic(cand.target_id(i))[r.ordinal()];
                    if(taxid > 0) {
                        long score = cand.hits(i);
                        totalscore += score;

                        Long it = scores.get(taxid);

                        if(it != null) {
                            scores.put(taxid, scores.get(taxid) + score);
                        }
                        else {
                            scores.put(taxid, score);
                        }

                    }
                }

//            std::cout << "\n    " << taxonomy::rank_name(r) << " ";

                //determine taxon id with most votes
                long toptid = 0;
                long topscore = 0;

                for(Map.Entry<Long, Long> x : scores.entrySet()) {

//                std::cout << x.first << ":" << x.second << ", ";

                    if(x.getValue() > topscore) {
                        toptid = x.getKey();
                        topscore = x.getValue();
                    }
                }

                //if enough candidates (weighted by their hits)
                //agree on a taxon => classify as such
                if(topscore >= (totalscore * trustedMajority)) {

//                std::cout << "  => classified " << taxonomy::rank_name(r)
//                          << " " << toptid << '\n';

                    return db.taxon_with_id(toptid);
                }

                scores.clear();
            }


        }

        //candidates couldn't agree on a taxon on any relevant taxonomic rank
        return null;

    }


    public void update_coverage_statistics(Classification result, Classification truth, ClassificationStatistics stats) {

        Long[] lin = this.db.ranks(truth.tax());


        //check if taxa are covered in DB
        for(long taxid : lin) {

            Taxonomy.Rank r = this.db.taxon_with_id(taxid).getRank();


            if(this.db.covers_taxon(taxid)) {
                if(r.ordinal() < result.rank().ordinal()) { //unclassified on rank
                    stats.count_coverage_false_neg(r);
                } else { //classified on rank
                    stats.count_coverage_true_pos(r);
                }
            }
            else {
                if(r.ordinal() < result.rank().ordinal()) { //unclassified on rank
                    stats.count_coverage_true_neg(r);
                } else { //classified on rank
                    stats.count_coverage_false_pos(r);
                }
            }
        }
    }

    public void show_ranks(BufferedWriter os, Database db, Long[] lineage,
                           EnumModes.taxon_print_mode mode, Taxonomy.Rank lowest, Taxonomy.Rank highest) {
        //one rank only
        try{
            //if(lowest == highest) { // ordinal?
            if(lowest.equals(highest)) {
                long taxid = lineage[lowest.ordinal()];
                os.write(Taxonomy.rank_name(lowest) +  ':');
                if(mode != EnumModes.taxon_print_mode.id_only) {
                    if(taxid > 1) {
                        os.write(db.taxon_with_id(taxid).getTaxonName());
                    }
                    else {
                        os.write("n/a");
                    }

                    os.newLine();

                    if(mode != EnumModes.taxon_print_mode.name_only) {
                        os.write("(" + taxid + ")");
                        os.newLine();
                    }
                }
                else {
                    os.write(Long.toString(taxid));
                    os.newLine();
                }
            }
            //range of ranks
            else {
                for(Taxonomy.Rank r = lowest; r.ordinal() <= highest.ordinal(); r = r.next()) {
                    long taxid = lineage[r.ordinal()];
                    if(taxid > 1) {
                        Taxon taxon = db.taxon_with_id(taxid);
                        if(taxon.getRank().ordinal() >= lowest.ordinal() && taxon.getRank().ordinal() <= highest.ordinal()) {
                            os.write(taxon.rank_name() + ':');
                            if(mode != EnumModes.taxon_print_mode.id_only) {
                                os.write(taxon.getTaxonName());
                                if(mode != EnumModes.taxon_print_mode.name_only) {
                                    os.write("(" + taxon.getTaxonId() + ")");
                                }
                            }
                            else {
                                os.write(Long.toString(taxon.getTaxonId()));
                            }
                        }
                        if(r.ordinal() < highest.ordinal()) {
                            os.write(',');
                        }
                        os.newLine();
                    }
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_ranks: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_ranks: "+ e.getMessage());
            System.exit(1);
        }

    }


    //-------------------------------------------------------------------
    public void show_matches(BufferedWriter os, Database db, TreeMap<LocationBasic, Integer> matches,
                      Taxonomy.Rank lowest)	{
        if(matches.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
                            '/' + r.getKey().getWindowId()+
                            ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
            else {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }

    }

    //-------------------------------------------------------------------
    public void show_matches_basic(BufferedWriter os, Database db, TreeMap<LocationBasic, Integer> matches,
                             Taxonomy.Rank lowest)	{
        if(matches.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
                            '/'+ r.getKey().getTargetId()+
                            '/' + r.getKey().getWindowId()+
                            ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
            else {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getValue()+
                            '/'+ r.getKey().getTargetId()+
                            '/' + r.getKey().getWindowId()+ ',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches_basic: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches_basic: "+ e.getMessage());
            System.exit(1);
        }

    }

    public void show_matches_native(BufferedWriter os, Database db, HashMap<LocationBasic, Integer> matches,
                                   Taxonomy.Rank lowest)	{
        if(matches.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
                            '/'+ r.getKey().getTargetId()+
                            '/' + r.getKey().getWindowId()+
                            ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
            else {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getValue() +
                            '/'+ r.getKey().getTargetId()+
                            '/' + r.getKey().getWindowId()+ ',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches_basic: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches_basic: "+ e.getMessage());
            System.exit(1);
        }

    }

    public void show_matches(BufferedWriter os, Database db, MatchesInWindow matchesWindow,
                             Taxonomy.Rank lowest)	{

        TreeMap<LocationBasic, Integer> matches = matchesWindow.getMatches();

        if(matches.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
                            '/'+ r.getKey().getTargetId()+
                            '/' + r.getKey().getWindowId()+
                            ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
            else {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getValue() +
                            '/'+ r.getKey().getTargetId()+
                            '/' + r.getKey().getWindowId()+ ',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }

    }

    public void show_matches_basic(BufferedWriter os, Database db, List<MatchCandidate> matchesWindow,
                             Taxonomy.Rank lowest)	{

        //TreeMap<LocationBasic, Integer> matches = matchesWindow.getMatches();

        if(matchesWindow.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(MatchCandidate r : matchesWindow) {
                    os.write(db.sequence_id_of_target(r.getTgt())+
                            ':'+ r.getTgt()+
                            '/' + r.getHits()+
                            ',');
                    os.newLine();
                }
            }
            else {
                for(MatchCandidate r : matchesWindow) {
                    long taxid = db.ranks_of_target(r.getTgt())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getTgt() +
                            '/'+ r.getTax().getTaxonName()+',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }

    }


    public void show_matches_list(BufferedWriter os, Database db, MatchesInWindowList matchesWindow,
                                   Taxonomy.Rank lowest)	{

        //TreeMap<LocationBasic, Integer> matches = matchesWindow.getMatches();
        List<MatchCandidate> matches = matchesWindow.getTop_list();

        if(matches.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(MatchCandidate r : matches) {
                    os.write(db.sequence_id_of_target(r.getTgt())+
                            '/'+ r.getTgt()+
                            '/' + r.getHits()+
                            ':' + r.getTax().getTaxonName() + ',');
                    os.newLine();
                }
            }
            else {
                for(MatchCandidate r : matches) {
                    long taxid = db.ranks_of_target(r.getTgt())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getHits() +
                            '/'+ r.getTgt()+
                            '/' + r.getTax().getTaxonName()+ ',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }

    }

    public void show_matches_native(BufferedWriter os, Database db, MatchesInWindowNative matchesWindow,
                                   Taxonomy.Rank lowest)	{

        HashMap<LocationBasic, Integer> matches = matchesWindow.getMatches();

        if(matches.isEmpty()) {
            return;
        }
        try {
            if(lowest == Taxonomy.Rank.Sequence) {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
                            '/' + r.getKey().getWindowId()+
                            ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
            else {
                for(Map.Entry<LocationBasic, Integer> r : matches.entrySet()) {
                    long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
                    os.write(Long.toString(taxid) + ':' + r.getValue() + ',');
                    os.newLine();
                }
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }

    }


    public void show_candidate_ranges(BufferedWriter os, Database db, MatchesInWindow cand) {

        int n = MatchesInWindow.maxNo;

        long w = db.getTargetWindowStride_();

        try {
            os.write("Showing candidate ranges=> n:"+n+", w:"+w);

            for(int i = 0; i < n; ++i) {
                //os.write("New: ");
                os.write("[" + (w * cand.window(i).getBeg())
                    + "," + (w * cand.window(i).getEnd() + "] "));
                os.newLine();
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
    }

    public void show_candidate_ranges(BufferedWriter os, Database db, List<LocationBasic> cand) {

        int n = MatchesInWindow.maxNo;

        long w = db.getTargetWindowStride_();

        try {
            /*for(int i = 0; i < n; ++i) {
                os.write('[' + (w * cand.window(i).getBeg())
                        + ',' + (w * cand.window(i).getEnd() + "] "));
                os.newLine();
            }*/

            for(LocationBasic current_location: cand) {
                os.write('[' + current_location.getTargetId()
                        + ',' + current_location.getWindowId() + "] ");
                os.newLine();
            }

        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
    }

    public void show_candidate_ranges(BufferedWriter os, Database db, MatchesInWindowNative cand) {

        int n = MatchesInWindow.maxNo;

        long w = db.getTargetWindowStride_();

        try {
            for(int i = 0; i < n; ++i) {
                os.write('[' + (w * cand.window(i).getBeg())
                        + ',' + (w * cand.window(i).getEnd() + "] "));
                os.newLine();
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
    }

    void show_classification(BufferedWriter os, Database db, Classification cls) {
        try {
            if(cls.sequence_level()) {
                //LOG.warn("Sequence Level");
                Taxonomy.Rank rmax = this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : this.param.getProperties().getLowestRank();

                //show_ranks_of_target(os, db, cls.target(),
                show_ranks_of_target(os, db, cls.tax(),
                        this.param.getProperties().getShowTaxaAs(), this.param.getProperties().getLowestRank(), rmax);
            }
            else if(cls.has_taxon()) {
                //LOG.warn("NO Sequence Level");
                if(cls.rank().ordinal() > this.param.getProperties().getHighestRank().ordinal()) {
                    //LOG.warn("NO Sequence Level e nada");
                    os.write("--");
                    os.newLine();
                }
                else {
                    //LOG.warn("NO Sequence Level e algo");
                    Taxonomy.Rank rmin = this.param.getProperties().getLowestRank().ordinal() < cls.rank().ordinal() ? cls.rank() : this.param.getProperties().getLowestRank();
                    Taxonomy.Rank rmax = this.param.getProperties().isShowLineage() ? this.param.getProperties().getHighestRank() : rmin;

                    show_ranks(os, db, db.ranks(cls.tax()),
                            this.param.getProperties().getShowTaxaAs(), rmin, rmax);
                }
            }
            else {
                //LOG.warn("NO Sequence Level e nada de nada");
                os.write("--");
                os.newLine();
            }
        }
        catch(IOException e) {
            LOG.error("IOException in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
        catch(Exception e) {
            LOG.error("Exception in function show_matches: "+ e.getMessage());
            System.exit(1);
        }
    }


}
