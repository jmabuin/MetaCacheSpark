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
import java.util.concurrent.*;


public class Query implements Serializable {

    private static final Log LOG = LogFactory.getLog(Query.class);

    // Default options values
    private MetaCacheOptions param;

    public Database db;
    private JavaSparkContext jsc;

    private EnumModes.InputFormat inputFormat;
    private HashMap<Location, Integer> hits;
    private ConcurrentSkipListMap<Taxon, Float> allTaxCounts;

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
            int sks = this.param.getProperties().getSketchlen();

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

        this.allTaxCounts = new ConcurrentSkipListMap<Taxon, Float>(new Comparator<Taxon>() {
            @Override
            public int compare(Taxon taxon, Taxon t1) {
                if(taxon.getRank().ordinal() < t1.getRank().ordinal()) {
                    return 1;
                }
                else if(taxon.getRank().ordinal() > t1.getRank().ordinal()) {
                    return -1;
                }
                else if(taxon.getTaxonId() < t1.getTaxonId()) {
                    return -1;
                }
                else if (taxon.getTaxonId() > t1.getTaxonId()) {
                    return 1;
                }

                return 0;
            }
        });

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
            //int numQueries = (this.param.getProperties().getPairing() == EnumModes.pairing_mode.none) ? stats.total() :
            int numQueries = (!this.param.isPaired_reads()) ? stats.total() :
                    2 * stats.total();

            double speed = (60.0 * (double)numQueries) / ((double)(endTime - initTime)/1e9);

            if (this.param.getAbundance_per() != Taxonomy.Rank.none) {
                this.db.estimate_abundance(this.allTaxCounts, this.param.getAbundance_per());
                this.show_abundance_estimates(d, stats.total());
            }

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

    //-------------------------------------------------------------------
    public void show_abundance_estimates(BufferedWriter d, long totalCount) {

        StringBuffer prefix = new StringBuffer();

        prefix.append("Estimated abundance (number of queries) per " + this.param.getAbundance_per().toString() + "\n");

        show_abundance_table(d, prefix, totalCount);
    }

    public void show_abundance_table(BufferedWriter d, StringBuffer prefix, long totalCount) {


        for(Map.Entry<Taxon, Float> tc : this.allTaxCounts.entrySet()) {
            if(tc.getKey() != this.db.getTaxa_().getNoTaxon_()) {
                prefix.append(tc.getKey().getRank().toString() + ":" + tc.getKey().getTaxonName() + "\t|\t");
            } else {
                prefix.append("none");
            }

            prefix.append(tc.getValue() + "\t|\t" + ((tc.getValue()/ (double)(totalCount)) * 100.0) + "%\n");

        }

        try {
            d.write(prefix.toString());
        }
        catch (IOException e) {
            e.printStackTrace();
            LOG.error("IO exception error in show_abundance_table: "+e.getMessage());
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

        this.db.adapt_options_to_database();

        //pair up reads from two consecutive files in the list
        for(int i = 0; i < infilenames.length; i += 2) {
            String fname1 = infilenames[i];
            String fname2 = infilenames[i+1];

            LOG.warn("Classifying file pairs on " + fname1 + " and " + fname2 );

            if(this.param.getNumThreads() > 1) {
                this.classify_pairs_multithread(fname1, fname2, d, stats);
            }
            else {
                this.classify_pairs(fname1, fname2, d, stats);
            }

        }

    }

    public void classify_per_file(String[] infilenames, BufferedWriter d, ClassificationStatistics stats) {


        try {

            for(int i = 0; i < infilenames.length; i++) {
                String fname = infilenames[i];

                LOG.warn("Processing single file "+fname);

                if(this.param.getNumThreads() > 1) {
                    this.classify_multithread(fname, d, stats);
                }
                else {
                    this.classify(fname, d, stats);
                }


            }
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_per_file: "+e.getMessage());
            System.exit(1);
        }

    }



    public void classify_pairs_multithread(String f1, String f2, BufferedWriter d, ClassificationStatistics stats) {

        LOG.warn("Entering classify_pairs_multithread");
        long initTime = System.nanoTime();
        long max_wait_time = Long.MAX_VALUE; // Max number of seconds to wait for threads to finish

        try {

            long totalReads = 0;
            long totalReads2 = 0;

            if (FilesysUtility.isFastaFile(f1) && FilesysUtility.isFastaFile(f2)) {
                totalReads = FilesysUtility.readsInFastaFile(f1);
                totalReads2 = FilesysUtility.readsInFastaFile(f1);

                //LOG.warn("Number of reads in " + f1 + " is " + totalReads +
                //        ", while number of reads in " + f2 + " is " + totalReads2 + ".");

                if (totalReads != totalReads2) {
                    System.exit(1);
                }

            }
            else if (FilesysUtility.isFastqFile(f1) && FilesysUtility.isFastqFile(f2)) {
                totalReads = FilesysUtility.readsInFastqFile(f1);
                totalReads2 = FilesysUtility.readsInFastqFile(f1);

                //LOG.warn("Number of reads in " + f1 + " is " + totalReads +
                //        ", while number of reads in " + f2 + " is " + totalReads2 + ".");
                if (totalReads != totalReads2) {

                    System.exit(1);
                }
            }
            else {
                LOG.error("Not recognized file format in " + f1 + " and " + f2);
                System.exit(1);
            }


            // Copy files to local in executors
            this.db.copy_files_to_local_for_query(f1, f2);

            long startRead;
            int bufferSize = this.param.getBuffer_size();

            SequenceFileReaderLocal seqReader = new SequenceFileReaderLocal(f1, 0, this.param);
            SequenceFileReaderLocal seqReader2 = new SequenceFileReaderLocal(f2, 0, this.param);

            //LOG.info("Sequence reader created. Current index: " + seqReader.getReadedValues());

            ThreadPoolExecutor executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(this.param.getNumThreads());
            Map<Integer, Classification> classifications = Collections.synchronizedMap(new HashMap<>());
            List<String> global_headers = new ArrayList<>();

            //boolean exit_loop = false;

            for(startRead = 0; startRead < totalReads; startRead+=bufferSize) {

                List<String> headers = new ArrayList<>();
                List<Integer> data = new ArrayList<>();
                List<Integer> data2 = new ArrayList<>();

                int current_thread = 0;

                for (long j = startRead; j < startRead + bufferSize; j++) {
                    SequenceData seq_data = seqReader.next();
                    SequenceData seq_data2 = seqReader2.next();

                    if ((seq_data == null) || (seq_data2 == null)) {
                        LOG.warn("Data is null!! for hits: " + j);
                        //exit_loop = true;
                        break;
                    }

                    data.add(seq_data.getData().length());
                    data2.add(seq_data2.getData().length());
                    headers.add(seq_data.getHeader());
                    global_headers.add(seq_data.getHeader());

                }


                executorPool.execute(new RunnableClassificationPaired(this,
                        classifications, f1, f2, "Thread" + current_thread + "Buf" + startRead,
                        (int) startRead, bufferSize, (int) this.db.getTargetWindowStride_(),
                        this.param,
                        data, data2, headers));

                if (current_thread >= this.param.getNumThreads()) {
                    current_thread = 0;
                } else {
                    current_thread++;
                }

                /*if (exit_loop) {
                    break;
                }*/

            }

            executorPool.shutdown();

            try {
                executorPool.awaitTermination(max_wait_time, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                System.out.println("InterruptedException " + e.getMessage());
            }

            for(int key = 0; key < classifications.size(); ++key) {

                this.print_classification(classifications.get(key), d, stats, global_headers.get(key));

            }

            classifications.clear();
            global_headers.clear();



            seqReader.close();
            seqReader2.close();

            long endTime = System.nanoTime();

            LOG.warn("[QUERY] Time in classify_pairs_multithread for " + this.param.getOutfile() + " is: " + ((endTime - initTime) / 1e9) + " seconds");
            //LOG.warn("Total characters readed: " + seqReader.getReadedValues());

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_pairs: "+e.getMessage());
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

                //LOG.warn("Number of reads in " + f1 + " is " + totalReads +
                //        ", while number of reads in " + f2 + " is " + totalReads2 + ".");

                if (totalReads != totalReads2) {
                    System.exit(1);
                }

            }
            else if (FilesysUtility.isFastqFile(f1) && FilesysUtility.isFastqFile(f2)) {
                totalReads = FilesysUtility.readsInFastqFile(f1);
                totalReads2 = FilesysUtility.readsInFastqFile(f1);

                //LOG.warn("Number of reads in " + f1 + " is " + totalReads +
                //        ", while number of reads in " + f2 + " is " + totalReads2 + ".");
                if (totalReads != totalReads2) {

                    System.exit(1);
                }
            }
            else {
                LOG.error("Not recognized file format in " + f1 + " and " + f2);
                System.exit(1);
            }


            // Copy files to local in executors
            this.db.copy_files_to_local_for_query(f1, f2);

            long startRead;
            int bufferSize = this.param.getBuffer_size();

            SequenceFileReaderLocal seqReader = new SequenceFileReaderLocal(f1, 0, this.param);
            SequenceFileReaderLocal seqReader2 = new SequenceFileReaderLocal(f2, 0, this.param);

            //LOG.info("Sequence reader created. Current index: " + seqReader.getReadedValues());

            SequenceData data;
            SequenceData data2;

            for(startRead = 0; startRead < totalReads; startRead+=bufferSize) {

                // Get corresponding hits for this buffer
                Map<Long, List<MatchCandidate>> hits;

                hits = this.db.accumulate_matches_paired(f1, f2,
                        startRead, bufferSize);

                long current_read;

                for(long i = 0;  i < hits.size() ; i++) {

                    current_read = startRead + i;
                    //Theoretically, number of sequences in data is the same as number of hits
                    data = seqReader.next();
                    data2 = seqReader2.next();

                    /*if((i == 0) || (i == hits.size()-1)) {
                        LOG.warn("Read " + i + " is " + data.getHeader() + " :: " + data.getData());
                    }*/

                    if((data == null) || (data2 == null)) {
                        LOG.warn("Data is null!! for hits: " + i + " and read " + (startRead + i));
                        break;
                    }

                    List<MatchCandidate> currentHits = hits.get(current_read);

                    /*for(MatchCandidate current_Cand: currentHits) {
                        LOG.warn("Best item: " +current_Cand.getTgt() + "::" + current_Cand.getHits());
                    }*/

                    this.process_database_answer(data.getHeader(), data.getData(),
                            data2.getData(), currentHits, d, stats);

                }




            }

            seqReader.close();
            seqReader2.close();

            long endTime = System.nanoTime();

            LOG.warn("[QUERY] Time in classify_pairs for " + this.param.getOutfile() + " is: " + ((endTime - initTime) / 1e9) + " seconds");
            //LOG.warn("Total characters readed: " + seqReader.getReadedValues());

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error in classify_pairs: "+e.getMessage());
            System.exit(1);
        }

    }


    public void classify(String filename, BufferedWriter d, ClassificationStatistics stats) {

        //LOG.warn("Entering classify");
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

            SequenceFileReaderLocal seqReader = new SequenceFileReaderLocal(filename, 0, this.param);

            LOG.info("Sequence reader created. Current index: " + seqReader.getReadedValues());

            SequenceData data;

            for(startRead = 0; startRead < totalReads; startRead+=bufferSize) {
                //while((currentRead < startRead+bufferSize) && ) {

                //LOG.warn("Parsing new reads block. Starting in: "+startRead + " and ending in  " + (startRead + bufferSize));


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


    public void classify_multithread(String f1, BufferedWriter d, ClassificationStatistics stats) {

        LOG.warn("Entering classify_multithread");
        long initTime = System.nanoTime();
        long max_wait_time = Long.MAX_VALUE; // Max number of seconds to wait for threads to finish

        try {

            long totalReads = 0;

            if (FilesysUtility.isFastaFile(f1)) {
                totalReads = FilesysUtility.readsInFastaFile(f1);


            }
            else if (FilesysUtility.isFastqFile(f1)) {
                totalReads = FilesysUtility.readsInFastqFile(f1);


            }
            else {
                LOG.error("Not recognized file format in " + f1);
                System.exit(1);
            }


            // Copy files to local in executors
            this.db.copy_files_to_local_for_query(f1, "");

            long startRead;
            int bufferSize = this.param.getBuffer_size();

            SequenceFileReaderLocal seqReader = new SequenceFileReaderLocal(f1, 0, this.param);

            //LOG.info("Sequence reader created. Current index: " + seqReader.getReadedValues());

            ThreadPoolExecutor executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(this.param.getNumThreads());
            Map<Integer, Classification> classifications = Collections.synchronizedMap(new HashMap<>());
            List<String> global_headers = new ArrayList<>();

            for(startRead = 0; startRead < totalReads; startRead+=bufferSize) {


                List<String> headers = new ArrayList<>();
                List<Integer> data = new ArrayList<>();

                int current_thread = 0;

                for (long j = startRead; j < startRead + bufferSize; j++) {
                    SequenceData seq_data = seqReader.next();

                    if (seq_data == null) {
                        LOG.warn("Data is null!! for hits: " + j);
                        break;
                    }

                    data.add(seq_data.getData().length());
                    headers.add(seq_data.getHeader());
                    global_headers.add(seq_data.getHeader());

                }


                executorPool.execute(new RunnableClassificationSingle(this,
                        classifications, f1, "Thread" + current_thread + "Buf" + startRead,
                        (int) startRead, bufferSize, (int) this.db.getTargetWindowStride_(),
                        this.param,
                        data, headers));

                if (current_thread >= this.param.getNumThreads()) {
                    current_thread = 0;
                    /*synchronized (this) {
                        for(int key: classifications.keySet()) {

                            this.print_classification(classifications.get(key), d, stats, global_headers.get(key));

                        }

                        classifications.clear();
                    }*/
                } else {
                    current_thread++;
                }


            }

            executorPool.shutdown();

            try {
                executorPool.awaitTermination(max_wait_time, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                System.out.println("InterruptedException " + e.getMessage());
            }

            //for (int i = 0; i < hits.size(); i++) {
            for(int key: classifications.keySet()) {


                //this.print_classification(classifications.get(key), d, stats, global_headers.get((int) key));

                //if(classifications.containsKey(key)) {
                this.print_classification(classifications.get(key), d, stats, global_headers.get(key));
                //}
                //else {
                //    LOG.warn("Processing " + key + " is null");
                //    this.print_classification(new Classification(), d, stats, global_headers.get((int) key));
                //}


            }

            classifications.clear();
            global_headers.clear();

            seqReader.close();

            long endTime = System.nanoTime();

            LOG.warn("[QUERY] Time in classify_pairs_best for " + this.param.getOutfile() + " is: " + ((endTime - initTime) / 1e9) + " seconds");
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

            reads = this.jsc.newAPIHadoopFile(pathToFile, FastqInputFormat.class, String.class, String.class, this.jsc.hadoopConfiguration());
            this.inputFormat = EnumModes.InputFormat.FASTQ;


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


        /*if(opt.output.makeTaxCounts && cls.best) {
                        ++buf.taxCounts[cls.best];
                    }*/
        if ((this.param.getAbundance_per() != Taxonomy.Rank.none) && (cls.has_taxon() && (cls.tax() != this.db.getTaxa_().getNoTaxon_()))) {
            Taxon best = cls.tax();

            if(!this.allTaxCounts.containsKey(best)) {
                this.allTaxCounts.put(best, 0F);
            }

            this.allTaxCounts.put(best, this.allTaxCounts.get(best) + 1);

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


    public Classification sequence_classification(MatchesInWindowList cand) {


        if (cand.getTop_list().isEmpty()) {
            //LOG.warn("Top list is empty!");
            return new Classification();
        }


        return new Classification(lowest_common_taxon(MatchesInWindowNative.maxNo, cand, (float) this.param.getProperties().getHitsDiff(),
                this.param.getProperties().getMergeBelow(), this.param.getProperties().getHighestRank()));



    }


    public Taxon lowest_common_taxon(int maxn, MatchesInWindowList cand,
                                     float trustedMajority, Taxonomy.Rank lowestRank,
                                     Taxonomy.Rank highestRank) {
        if(lowestRank == null) {
            lowestRank = Taxonomy.Rank.Sequence;
        }

        if(highestRank == null) {
            highestRank = Taxonomy.Rank.Domain;
        }



        double threshold = cand.getTop_list().get(0).getHits() > this.param.getProperties().getHitsMin() ?
                (cand.getTop_list().get(0).getHits() - this.param.getProperties().getHitsMin()) *
                        this.param.getProperties().getHitsDiffFraction() : 0;


        //int best_pos = cand.getTop_list().size() - 1;
        Taxon lca = db.taxon_of_target((long)cand.getTop_list().get(0).getTgt());
        //LOG.warn("Best taxon is: " + lca.getTaxonName() + ", hits: " + cand.getTop_list().get(0).getHits() + " threshold is: " + threshold);

        if(cand.getTop_list().get(0).getHits() < this.param.getProperties().getHitsMin()) {
            return this.db.getTaxa_().getNoTaxon_();
        }

        for (int i = 1; i < cand.getTop_list().size(); ++i) {

            if(cand.getTop_list().get(i).getHits() > threshold) {
                lca = this.db.ranked_lca(lca, cand.getTop_list().get(i).getTax());
                //LOG.warn("Obtained LCA: " + lca.getTaxonName());
                if (lca == null || lca == this.db.getTaxa_().getNoTaxon_() || lca.getRank().ordinal() > this.param.getProperties().getHighestRank().ordinal()) {
                    return this.db.getTaxa_().getNoTaxon_();
                }
            }
            else {
                break;
            }




        }

        //LOG.warn("Final LCA is: " + lca.getTaxonName());

        //if(lowestRank.ordinal() > lca.getRank().ordinal()) {
        //    return this.db.getTaxa_().getNoTaxon_();
        //}

        while ((lowestRank.ordinal() > lca.getRank().ordinal()) && (lca.getParentId()!=0)) {

            lca = this.db.getTaxa_().getTaxa_().get(lca.getParentId());
        }

        if(lca == null) {
            return this.db.getTaxa_().getNoTaxon_();
        }

        return lca;

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


    public void show_matches_list(BufferedWriter os, Database db, MatchesInWindowList matchesWindow,
                                   Taxonomy.Rank lowest)	{

        //TreeMap<LocationBasic, Integer> matches = matchesWindow.getMatches();
        List<MatchCandidate> matches = matchesWindow.getTop_list();

        if(matches.isEmpty()) {
            return;
        }
        try {

            if(lowest == Taxonomy.Rank.Sequence) {
                os.write("\t");
                for(MatchCandidate r : matches) {
                    os.write(r.getTax().getTaxonId()+"["+
                            r.getTgt()+"]"+
                            ':' + r.getHits()+',');
                    //os.newLine();
                }
                os.write("\t");
            }
            else {
                os.write("\t");
                for(MatchCandidate r : matches) {
                    //long taxid = db.ranks_of_target(r.getTgt())[lowest.ordinal()];
                    os.write(r.getTax().getTaxonId()+":" + r.getHits()+",");
                    //os.newLine();
                }
                os.write("\t");
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


    public void print_classification(Classification cls, BufferedWriter d, ClassificationStatistics stats, String header) {

        if (cls == null) {
            LOG.warn("cls is null for header: " + header);
        }

        if (cls.rank() == null) {
            LOG.warn("rank is null");
        }

        if (stats == null) {
            LOG.warn("stats is null for header: " + header);
        }

        stats.assign(cls.rank());

        /*if(opt.output.makeTaxCounts && cls.best) {
                        ++buf.taxCounts[cls.best];
                    }*/
        if ((this.param.getAbundance_per() != Taxonomy.Rank.none) && (cls.has_taxon() && (cls.tax() != this.db.getTaxa_().getNoTaxon_()))) {
            Taxon best = cls.tax();

            if(!this.allTaxCounts.containsKey(best)) {
                this.allTaxCounts.put(best, 0F);
            }

            this.allTaxCounts.put(best, this.allTaxCounts.get(best) + 1);

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

                show_classification(d, this.db, cls);

            }

    } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
