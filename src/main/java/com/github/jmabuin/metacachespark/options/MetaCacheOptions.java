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

package com.github.jmabuin.metacachespark.options;

import com.github.jmabuin.metacachespark.EnumModes;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

public class MetaCacheOptions implements Serializable {

	private static final Log LOG = LogFactory.getLog(MetaCacheOptions.class);

	private Options options = null;
/*
	public enum Mode { HELP, QUERY, BUILD, ADD, INFO, ANNOTATE}

	public enum pairing_mode { none, files, sequences} // Pairing of queries

	public enum align_mode {none, semi_global} // Alignment mode

	public enum map_view_mode { none, mapped_only, all} // How to show mapping

	public enum taxon_print_mode { name_only, id_only, id_name} // how taxon formatting will be done

	public enum InputFormat {FASTA, FASTQ}
*/
	private EnumModes.Mode operation_mode;
	//private EnumModes.DatabaseType database_type;
	private EnumModes.QueryMode query_mode;
	private String taxonomy;
	private int partitions = 1;
	private String configuration;
    private int buffer_size = 51200;
    //private int result_size = 0;
	private MetaCacheProperties properties;

    private String dbfile			= "";
    private String infiles			= "";
    private String outfile         = "";
    private String[] infiles_query;

    private int numThreads = 1;
    //private boolean myWholeTextFiles = false;
    private boolean paired_reads = false;
    //private boolean metacache_like = false;
    //private boolean metacache_like_input = false;
    //private boolean simple = false;
    private int hits_greater_than = 2;
    private boolean remove_overpopulated_features = false;

    private Taxonomy.Rank abundance_per = Taxonomy.Rank.none;

	private String correctUse =
			"spark-submit --class com.github.metachachespark.MetaCacheSpark MetaCacheSpark-0.4.0.jar";// [SparkBWA Options] Input.fastq [Input2.fastq] Output\n";


	// Header to show when the program is not launched correctly
	private String header = "\tMetaCacheSpark performs metagenomics analysis by means of Apache Spark and the metacache minhashing algorithm.\nAvailable parameters:\n";

	// Footer to show when the program is not launched correctly
	private String footer = "\nPlease report issues at josemanuel.abuin@usc.es";

	private String[] otherOptions;

	public MetaCacheOptions(String[] args) {

		//Parse arguments
		for (String argument : args) {
			LOG.info("["+this.getClass().getName()+"] :: Received argument: " + argument);
		}

		this.options = this.initOptions();

		//Parse the given arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		this.operation_mode = EnumModes.Mode.HELP;

		try {
			cmd = parser.parse(this.options, args, true);

			//We look for the operation mode
			if (cmd.hasOption('h') || cmd.hasOption("help")) {
				//Case of showing the help
				this.operation_mode = EnumModes.Mode.HELP;
			}
			else {
			    if (cmd.hasOption('m') || cmd.hasOption("mode")) {
                    // Choose the mode
                    String selected_mode = cmd.getOptionValue("mode");

                    switch (selected_mode) {
                        case "build":
                            this.operation_mode = EnumModes.Mode.BUILD;
                            break;
                        case "query":
                            this.operation_mode = EnumModes.Mode.QUERY;
                            break;
                        case "add":
                            this.operation_mode = EnumModes.Mode.ADD;
                            break;
                        case "info":
                            this.operation_mode = EnumModes.Mode.INFO;
                            break;
                        case "annotate":
                            this.operation_mode = EnumModes.Mode.ANNOTATE;
                            break;
                        default:
                            this.operation_mode = EnumModes.Mode.HELP;
                            break;

                    }
                }
                else {
                    this.operation_mode = EnumModes.Mode.HELP;
                }

                /*
                if (cmd.hasOption('d') || cmd.hasOption("database_type")) {

                    String selected_database_type = cmd.getOptionValue("database_type");

                    switch (selected_database_type) {
                        case "hashmap":
                            this.database_type = EnumModes.DatabaseType.HASHMAP;
                            break;
                        case "hashmultimap":
                            this.database_type = EnumModes.DatabaseType.HASHMULTIMAP_GUAVA;
                            break;
                        case "hashmultimap_native":
                            this.database_type = EnumModes.DatabaseType.HASHMULTIMAP_NATIVE;
                            break;
                        case "parquet":
                            this.database_type = EnumModes.DatabaseType.PARQUET;
                            break;
                        case "combine":
                            this.database_type = EnumModes.DatabaseType.COMBINE_BY_KEY;
                            break;
                        default:
                            this.database_type = EnumModes.DatabaseType.HASHMAP;
                            break;
                    }
                }
                else {
                    this.database_type = EnumModes.DatabaseType.HASHMAP;
                }
                */
                if ( cmd.hasOption('t') || cmd.hasOption("taxonomy")) {
                    this.taxonomy = cmd.getOptionValue("taxonomy");
                }

                if ( cmd.hasOption('p') || cmd.hasOption("partitions")) {
                    this.partitions = Integer.parseInt(cmd.getOptionValue("partitions"));
                }

                if ( cmd.hasOption('c') || cmd.hasOption("configuration")) {
                    this.configuration = cmd.getOptionValue("configuration");
                    this.getPropertiesFromFile();
                }
                else {
                    this.getDefaultProperties();
                }

                if (cmd.hasOption('b') || cmd.hasOption("buffer_size")) {
                    this.buffer_size = Integer.parseInt(cmd.getOptionValue("buffer_size"));
                }
                /*
                if (cmd.hasOption('w') || cmd.hasOption("myWholeTextFiles")) {
                    this.myWholeTextFiles = true;
                }

                if (cmd.hasOption('l') || cmd.hasOption("metacache_like")) {
                    this.metacache_like = true;
                }

                if (cmd.hasOption('i') || cmd.hasOption("metacache_like_input")) {
                    this.metacache_like_input = true;
                }
                */
                if (cmd.hasOption('r') || cmd.hasOption("paired_reads")) {
                    this.paired_reads = true;
                }

                if (cmd.hasOption('n') || cmd.hasOption("num_threads")) {
                    this.numThreads = Integer.parseInt(cmd.getOptionValue("num_threads"));
                }
                /*
                if (cmd.hasOption('s') || cmd.hasOption("simple")) {
                    this.simple = true;
                }
                */
                /*if (cmd.hasOption('e') || cmd.hasOption("repartition")) {
                    this.repartition = true;
                }*/

                //-abundance-per
                if (cmd.hasOption('a') || cmd.hasOption("abundance_per")) {
                    this.abundance_per = Taxonomy.rank_from_name(cmd.getOptionValue("abundance_per"));
                    //LOG.warn("Abundance per set to: " + cmd.getOptionValue("abundance_per"));
                }

                //-remove-overpopulated-features
                if (cmd.hasOption('o') || cmd.hasOption("remove_overpopulated_features")) {
                    this.remove_overpopulated_features = true;
                }

                if (cmd.hasOption('g') || cmd.hasOption("hits_greater_than")) {
                    this.hits_greater_than = Integer.parseInt(cmd.getOptionValue("hits_greater_than"));
                }

                if (cmd.hasOption('q') || cmd.hasOption("query_mode")) {

                    String selected_database_type = cmd.getOptionValue("query_mode");

                    switch (selected_database_type) {
                        case "precise":
                            this.query_mode = EnumModes.QueryMode.PRECISE;
                            break;
                        case "threshold":
                            this.query_mode = EnumModes.QueryMode.THRESHOLD;
                            break;
                        case "fast":
                            this.query_mode = EnumModes.QueryMode.FAST;
                            break;
                        case "very_fast":
                            this.query_mode = EnumModes.QueryMode.VERY_FAST;
                            break;
                        default:
                            this.query_mode = EnumModes.QueryMode.FAST;
                            break;
                    }
                }
                else {
                    this.query_mode = EnumModes.QueryMode.FAST;
                }


            }

			// Get and parse the rest of the arguments
			this.otherOptions = cmd.getArgs(); //With this we get the rest of the arguments

			if(this.operation_mode == EnumModes.Mode.BUILD) {
                // Check if the numbe rof arguments is correct. This is, dbname and infiles
                if (this.otherOptions.length != 2) {
                    LOG.error("["+this.getClass().getName()+"] No input reference and output database name have been found. Aborting.");

                    for (String tmpString : this.otherOptions) {
                        LOG.error("["+this.getClass().getName()+"] Other args:: " + tmpString);
                    }

                    //formatter.printHelp(correctUse, header, options, footer, true);
                    System.exit(1);
                }
                else {

                    this.dbfile 	= this.otherOptions[0];
                    this.infiles 	= this.otherOptions[1];

                }
			}
			else if (this.operation_mode == EnumModes.Mode.QUERY) {
                // Check if the number of arguments is correct. This is, dbname, outfile and infiles
                LOG.info("Query mode...");
                if (this.otherOptions.length < 3) {
                    LOG.error("["+this.getClass().getName()+"] No database, input data and output file name have been found. Aborting.");

                    for (String tmpString : this.otherOptions) {
                        LOG.error("["+this.getClass().getName()+"] Other args:: " + tmpString);
                    }

                    //formatter.printHelp(correctUse, header, options, footer, true);
                    System.exit(1);
                }
                else {

                    this.dbfile 	= this.otherOptions[0];
                    this.outfile    = this.otherOptions[1];
                    this.infiles_query = Arrays.copyOfRange(this.otherOptions, 2, this.otherOptions.length);

                }
			}

		}
		catch (UnrecognizedOptionException e) {
			e.printStackTrace();
			//formatter.printHelp(correctUse, header, options, footer, true);

			System.exit(1);


		} catch (MissingOptionException e) {
			//formatter.printHelp(correctUse, header, options, footer, true);
			System.exit(1);
		} catch (ParseException e) {
			//formatter.printHelp( correctUse,header, options,footer , true);
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Function to init the MetaCache available options
	 *
	 * @return An Options object containing the available options
	 */
	public Options initOptions() {

		Options privateOptions = new Options();

		//OptionGroup modes = new OptionGroup();

        // Used options: h, m, d, t, p, c, b, w, l, i, r, n, s, e, a, o, g
		Option help = new Option("h","help", false,"Shows documentation");
		privateOptions.addOption(help);

        Option mode = new Option("m", "mode", true, "Operation mode to use with MetaCacheSpark.\nAvailable options are: build, query, add, info, annotate.");
        privateOptions.addOption(mode);

        /*Option database_type = new Option("d", "database_type", true, "Construction method of the database to be used.\nAvailable options: hashmap, hashmultimap," +
                "hashmultimap_native, parquet, combine.");
        privateOptions.addOption(database_type);*/

        Option taxonomy = new Option("t", "taxonomy", true, "[BUILD] Path to the taxonomy to be used in the HDFS.");
        //taxonomy.setRequired(true);
        privateOptions.addOption(taxonomy);

        Option partitions =  new Option("p", "partitions", true, "[BUILD|QUERY] Number of partitions to use.");
        privateOptions.addOption(partitions);

        Option configuration = new Option("c", "configuration", true, "[BUILD|QUERY] Configuration file with parameters to be used inside MetaCacheSpark.");
        privateOptions.addOption(configuration);

        Option buffer_size = new Option("b", "buffer_size", true, "[QUERY] Buffer size to perform query operations. It indicates the number of sequences to be query by each thread.");
        privateOptions.addOption(buffer_size);

        //Option myWholeTextFiles = new Option("w", "myWholeTextFiles", false, "Use customize wholetextfiles or not");
        //privateOptions.addOption(myWholeTextFiles);

        //Option metacache_like = new Option("l", "metacache_like", false, "Create the database in the most similar way to metacache");
        //privateOptions.addOption(metacache_like);

        //Option metacache_like_input = new Option("i", "metacache_like_input", false, "Reads the input sequences in alpabetic order, just like metacache (only for building)");
        //privateOptions.addOption(metacache_like_input);

        Option paired_reads = new Option("r", "paired_reads", false, "[QUERY] Use paired reads in the classification or not.");
        privateOptions.addOption(paired_reads);

        Option num_threads = new Option("n", "num_threads", true, "[QUERY] Number of threads per executor to use in the classification phase.");
        privateOptions.addOption(num_threads);

        //Option simple = new Option("s", "simple", false, "Builds database in simple mode (only for building)");
        //privateOptions.addOption(simple);

        Option repartition = new Option("e", "repartition", false, "[BUILD] Uses Spark repartition method to repartition sequences among executors.");
        privateOptions.addOption(repartition);

        Option abundance_per = new Option("a", "abundance_per", true, "[QUERY] Indicates if use the abundance estimation feature and at which level.");
        privateOptions.addOption(abundance_per);

        Option remove_overpopulated_features = new Option("o", "remove_overpopulated_features", false, "[BUILD] Uses remove overpopulated features when building.");
        privateOptions.addOption(remove_overpopulated_features);

        Option hits_greater_than = new Option("g", "hits_greater_than", true, "[QUERY] Gets candidates with more than specified hits in the classification maps.");
        privateOptions.addOption(hits_greater_than);

        Option query_mode = new Option("q", "query_mode", true, "[QUERY] Mode selected for query. Available options are: precise, threshold, fast, very_fast. Default: threshold");
        privateOptions.addOption(query_mode);

		return privateOptions;
	}

	public EnumModes.Mode getMode() {
		return this.operation_mode;
	}

	public void printHelp() {
		//To print the help
		HelpFormatter formatter = new HelpFormatter();
		//formatter.setWidth(500);
		formatter.printHelp( correctUse,header, this.options,footer , true);

	}

	public String[] getOtherOptions() {
		return this.otherOptions;
	}

	private void getPropertiesFromFile() {
        InputStream inputStream ;

        try {
            Properties prop = new Properties();
            String propFileName = this.configuration;

            File initialFile = new File(propFileName);
            inputStream = new FileInputStream(initialFile);

            prop.load(inputStream);

            this.properties = new MetaCacheProperties(prop);

            inputStream.close();
        } catch (IOException e) {
            System.out.println("IOException: " + e);
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }

    private void getDefaultProperties() {

        InputStream inputStream;

        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("Property file '" + propFileName + "' not found in the classpath");
            }

            this.properties = new MetaCacheProperties(prop);

            inputStream.close();
        } catch (IOException e) {
            System.out.println("IOException: " + e);
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }


    }

    public EnumModes.Mode getOperation_mode() {
        return operation_mode;
    }
/*
    public EnumModes.DatabaseType getDatabase_type() {
        return database_type;
    }
*/
    public String getTaxonomy() {
        return taxonomy;
    }

    public int getPartitions() {
        return partitions;
    }

    public String getConfiguration() {
        return configuration;
    }

    public MetaCacheProperties getProperties() {
        return properties;
    }

    public String getDbfile() {
        return dbfile;
    }

    public String getInfiles() {
        return infiles;
    }

    public String getOutfile() {
        return outfile;
    }

    public String[] getInfiles_query() {
        return infiles_query;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public int getBuffer_size() {
        return buffer_size;
    }

    /*public boolean isMyWholeTextFiles() {
        return myWholeTextFiles;
    }*/

    public boolean isPaired_reads() {
        return paired_reads;
    }

    /*public boolean isMetacache_like() {
        return metacache_like;
    }

    public boolean isMetacache_like_input() {
        return metacache_like_input;
    }

    public boolean isSimple() {
        return simple;
    }

    public void setSimple(boolean simple) {
        this.simple = simple;
    }*/

    /*public boolean isRepartition() {
        return repartition;
    }*/

    public Taxonomy.Rank getAbundance_per() {
        return abundance_per;
    }

    public boolean isRemove_overpopulated_features() {
        return remove_overpopulated_features;
    }

    public int getHits_greater_than() {
        return hits_greater_than;
    }

    public EnumModes.QueryMode getQuery_mode() {
        return query_mode;
    }
}
