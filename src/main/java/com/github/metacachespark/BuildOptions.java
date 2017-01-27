package com.github.metacachespark;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

/**
 * Created by chema on 1/24/17.
 */
public class BuildOptions implements Serializable {

	private static final Log LOG = LogFactory.getLog(BuildOptions.class);

	private Options options = null;

	// Default options values
	private int sketchlenValue 		= 16;
	private int winlenValue 		= 128;
	private int winstrideValue 		= 113;
	private int kmerlenValue 		= 16;
	private boolean verboseValue 	= false;
	private String taxonomyValue 	= "";
	private String taxpostmapValue 	= "";

	private float maxLoadFactorValue 		= -1.0f;
	private int maxLocationsPerFeatureValue = 0;
	private Taxonomy.Rank removeAmbigFeaturesOnRank = Taxonomy.Rank.none;

	private int maxTaxaPerFeature = 1;

	private String dbfile			= "";
	private String infiles			= "";

	private TaxonomyParam taxonomyParam;

	private String[] otherOptions;
	private int numPartitions = 1;


	public BuildOptions(String args[]) {

		//Parse arguments
		for (String argument : args) {
			LOG.info("["+this.getClass().getName()+"] :: Received argument: " + argument);
		}

		this.options = this.initOptions(args);

		//Parse the given arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;


		try {
			cmd = parser.parse(this.options, args);

			//We check the options

			if (cmd.hasOption('s') || cmd.hasOption("sketchlen")) {
				//Case of sketchlen
				this.sketchlenValue = Integer.parseInt(cmd.getOptionValue("index"));

			}

			if (cmd.hasOption('w') || cmd.hasOption("winlen")) {
				// Case of winlen
				this.winlenValue = Integer.parseInt(cmd.getOptionValue("winlen"));

			}

			if (cmd.hasOption('d') || cmd.hasOption("winstride")) {
				// Case of winstride
				this.winstrideValue = Integer.parseInt(cmd.getOptionValue("winstride"));

			}

			if (cmd.hasOption('k') || cmd.hasOption("kmerlen")) {
				// Case of kmerlen
				this.kmerlenValue = Integer.parseInt(cmd.getOptionValue("kmerlen"));

			}

			if (cmd.hasOption('v') || cmd.hasOption("verbose")) {
				// Case of verbose
				this.verboseValue = true;

			}

			if (cmd.hasOption('t') || cmd.hasOption("taxonomy")) {
				// Case of taxonomy
				this.taxonomyValue = cmd.getOptionValue("taxonomy");

			}

			if (cmd.hasOption('p') || cmd.hasOption("taxpostmap")) {
				// Case of taxpostmap
				this.taxpostmapValue = cmd.getOptionValue("taxpostmap");

			}

			if (cmd.hasOption('f') || cmd.hasOption("max_load_fac")) {
				// Case of max_load_fac
				this.maxLoadFactorValue = Float.parseFloat(cmd.getOptionValue("max_load_fac"));

			}

			if (cmd.hasOption('r') || cmd.hasOption("max_locations_per_feature")) {
				// Case of max_locations_per_feature
				this.maxLocationsPerFeatureValue = Integer.parseInt(cmd.getOptionValue("max_locations_per_feature"));

			}

			if (cmd.hasOption('b') || cmd.hasOption("remove_ambig_features")) {
				// Case of max_load_fac
				this.removeAmbigFeaturesOnRank = Taxonomy.rank_from_name(cmd.getOptionValue("remove_ambig_features"));

			}

			if (cmd.hasOption('e') || cmd.hasOption("max_ambig_per_feature")) {
				// Case of max_load_fac
				this.maxTaxaPerFeature = Integer.parseInt(cmd.getOptionValue("max_ambig_per_feature"));

			}

			/*
			if(this.taxpostmapValue != "" && this.taxonomyValue != "") {
				this.taxonomyParam = new TaxonomyParam(this.taxonomyValue, this.taxpostmapValue);
			}
			*/
			if(this.taxonomyValue != "") {
				this.taxonomyParam = new TaxonomyParam(this.taxonomyValue, this.taxpostmapValue);
			}


			if (cmd.hasOption('l') || cmd.hasOption("num_partitions")) {
				// Case of max_load_fac
				this.numPartitions = Integer.parseInt(cmd.getOptionValue("num_partitions"));

			}

			// Get and parse the rest of the arguments
			this.otherOptions = cmd.getArgs(); //With this we get the rest of the arguments

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


	private Options initOptions(String[] args) {
		Options privateOptions = new Options();

		//OptionGroup buildOptions = new OptionGroup();

		/*
		Previous options from main program are:
			if (cmd.hasOption('h') || cmd.hasOption("help")) {
				//Case of showing the help
				this.mode = Mode.HELP;
			} else if (cmd.hasOption('q') || cmd.hasOption("query")) {
				// Case of query
				this.mode = Mode.QUERY;
			} else if (cmd.hasOption('b') || cmd.hasOption("build")) {
				// Case of build
				this.mode = Mode.BUILD;
			} else if (cmd.hasOption('a') || cmd.hasOption("add")) {
				// Case of add
				this.mode = Mode.ADD;
			} else if (cmd.hasOption('i') || cmd.hasOption("info")) {
				// Case of info
				this.mode = Mode.INFO;
			} else if (cmd.hasOption('n') || cmd.hasOption("annotate")) {
				// Case of annotate
				this.mode = Mode.ANNOTATE;
			}
		 */

		Option sketchlen = new Option("s","sketchlen", true,"Shows documentation");
		//buildOptions.addOption(sketchlen);
		privateOptions.addOption(sketchlen);

		Option winlen = new Option("w","winlen", true,"Classify read sequences using pre-built database");
		//buildOptions.addOption(winlen);
		privateOptions.addOption(winlen);

		Option winstride = new Option("d", "winstride", true, "Build new database from reference genomes");
		//buildOptions.addOption(winstride);
		privateOptions.addOption(winstride);

		Option kmerlen = new Option("k", "kmerlen", true, "Add reference genomes and/or taxonomy to existing database");
		//buildOptions.addOption(kmerlen);
		privateOptions.addOption(kmerlen);

		Option verbose = new Option("v", "verbose", false, "Shows database and reference genome properties");
		//buildOptions.addOption(verbose);
		privateOptions.addOption(verbose);

		Option taxonomy = new Option("t", "taxonomy", true, "Annotate sequences with taxonomic information");
		//buildOptions.addOption(taxonomy);
		privateOptions.addOption(taxonomy);

		Option taxpostmap = new Option("p", "taxpostmap", true, "Shows database and reference genome properties");
		//buildOptions.addOption(taxpostmap);
		privateOptions.addOption(taxpostmap);

		Option max_load_fac = new Option("f", "max_load_fac", true,"Maximum value for load factor");
		//buildOptions.addOption(max_load_fac);
		privateOptions.addOption(max_load_fac);

		Option max_locations_per_feature = new Option("r", "max_locations_per_feature", true, "Maximum number of locations per feature");
		//buildOptions.addOption(max_locations_per_feature);
		privateOptions.addOption(max_locations_per_feature);

		Option remove_ambig_features = new Option("z", "remove_ambig_features", true, "Remove ambiguous features on rank");
		//buildOptions.addOption(remove_ambig_features);
		privateOptions.addOption(remove_ambig_features);

		Option max_ambig_per_feature = new Option("e","max_ambig_per_feature", true, "Maximim ambiguous per feature" );
		//buildOptions.addOption(max_ambig_per_feature);
		privateOptions.addOption(max_ambig_per_feature);

		Option num_partitions = new Option("l","num_partitions", true,"Number of desired partitions to parallelize");
		//buildOptions.addOption(num_partitions);
		privateOptions.addOption(num_partitions);

		//privateOptions.addOptionGroup(buildOptions);

		return privateOptions;

	}

	public int getSketchlenValue() {
		return sketchlenValue;
	}

	public void setSketchlenValue(int sketchlenValue) {
		this.sketchlenValue = sketchlenValue;
	}

	public int getWinlenValue() {
		return winlenValue;
	}

	public void setWinlenValue(int winlenValue) {
		this.winlenValue = winlenValue;
	}

	public int getWinstrideValue() {
		return winstrideValue;
	}

	public void setWinstrideValue(int winstrideValue) {
		this.winstrideValue = winstrideValue;
	}

	public int getKmerlenValue() {
		return kmerlenValue;
	}

	public void setKmerlenValue(int kmerlenValue) {
		this.kmerlenValue = kmerlenValue;
	}

	public boolean isVerboseValue() {
		return verboseValue;
	}

	public void setVerboseValue(boolean verboseValue) {
		this.verboseValue = verboseValue;
	}

	public String getTaxonomyValue() {
		return taxonomyValue;
	}

	public void setTaxonomyValue(String taxonomyValue) {
		this.taxonomyValue = taxonomyValue;
	}

	public String getTaxpostmapValue() {
		return taxpostmapValue;
	}

	public void setTaxpostmapValue(String taxpostmapValue) {
		this.taxpostmapValue = taxpostmapValue;
	}

	public float getMaxLoadFactorValue() {
		return maxLoadFactorValue;
	}

	public void setMaxLoadFactorValue(float maxLoadFactorValue) {
		this.maxLoadFactorValue = maxLoadFactorValue;
	}

	public int getMaxLocationsPerFeatureValue() {
		return maxLocationsPerFeatureValue;
	}

	public void setMaxLocationsPerFeatureValue(int maxLocationsPerFeatureValue) {
		this.maxLocationsPerFeatureValue = maxLocationsPerFeatureValue;
	}

	public Taxonomy.Rank getRemoveAmbigFeaturesOnRank() {
		return removeAmbigFeaturesOnRank;
	}

	public void setRemoveAmbigFeaturesOnRank(Taxonomy.Rank removeAmbigFeaturesOnRank) {
		this.removeAmbigFeaturesOnRank = removeAmbigFeaturesOnRank;
	}

	public int getMaxTaxaPerFeature() {
		return maxTaxaPerFeature;
	}

	public void setMaxTaxaPerFeature(int maxTaxaPerFeature) {
		this.maxTaxaPerFeature = maxTaxaPerFeature;
	}

	public String getDbfile() {
		return dbfile;
	}

	public void setDbfile(String dbfile) {
		this.dbfile = dbfile;
	}

	public String getInfiles() {
		return infiles;
	}

	public void setInfiles(String infiles) {
		this.infiles = infiles;
	}

	public TaxonomyParam getTaxonomyParam() {
		return taxonomyParam;
	}

	public void setTaxonomyParam(TaxonomyParam taxonomyParam) {
		this.taxonomyParam = taxonomyParam;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}

}
