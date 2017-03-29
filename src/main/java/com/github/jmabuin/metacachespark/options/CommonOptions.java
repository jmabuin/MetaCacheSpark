package com.github.jmabuin.metacachespark.options;

import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.TaxonomyParam;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

/**
 * Created by chema on 1/24/17.
 */
public class CommonOptions implements Serializable {

	private static final Log LOG = LogFactory.getLog(CommonOptions.class);

	private Options options = null;

	// Default options values
	/*
     * Query sampling scheme
     */
	private int sketchlen 	= 16;  //< 0 : use value from database
	private int winlen    	= 128;  //< 0 : use value from database
	private int winstride	= 113;  //< 0 : use value from database
	private int kmerlen 	= 16;
	/*
     * Tuning parameters
     */
	private double max_load_fac        = -1; //< 0 : use value from database
	private int maxTargetsPerSketchVal  = 16; //< 0 : use value from database
	private int max_locations_per_feature = 256;

	private boolean verboseValue 	= false;

	//private String[] otherOptions;
	private int numPartitions = 1;

	private boolean buildModeHashMap = false;
	private boolean buildModeHashMultiMapG = false;
	private boolean buildModeHashMultiMapMC = false;
	private boolean buildModeParquetDataframe = true;
	private boolean buildCombineByKey = false;

	public CommonOptions() {

		this.options = this.initOptions();

	}


	private Options initOptions() {
		Options privateOptions = new Options();


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

		Option max_locations_per_feature = new Option("r", "max_locations_per_feature", true, "Maximum number of locations per feature");
		privateOptions.addOption(max_locations_per_feature);

		Option verbose = new Option("v", "verbose", false, "Shows database and reference genome properties");
		privateOptions.addOption(verbose);

		Option max_load_fac = new Option("f", "max_load_fac", true,"Maximum value for load factor");
		privateOptions.addOption(max_load_fac);

		Option maxTargetsPerSketchVal = new Option("m", "maxTargetsPerSketchVal", true, "Max of targets per sketch");
		privateOptions.addOption(maxTargetsPerSketchVal);

		Option num_partitions = new Option("l","num_partitions", true,"Number of desired partitions to parallelize");
		privateOptions.addOption(num_partitions);

		// Group for build
		OptionGroup buildMode = new OptionGroup();

		Option hashmap = new Option("p", "hashmap", false, "Uses distributed Java hashmaps to build the database");
		buildMode.addOption(hashmap);

		Option hashmultimap = new Option("g", "hashmultimap", false, "Uses distributed Guava hashmultimaps to build the database");
		buildMode.addOption(hashmultimap);

		Option hashmultimapMC = new Option("o", "hashmultimapMC", false, "Uses own implementation distributed hashmultimap to build the database");
		buildMode.addOption(hashmultimapMC);

		Option dataframe = new Option("e", "dataframe", false, "Uses Spark parquet and dataframes to build the database");
		buildMode.addOption(dataframe);

		Option combine = new Option("c", "combine", false, "Uses Spark combineByKey to build the database");
		buildMode.addOption(combine);

		privateOptions.addOptionGroup(buildMode);

		return privateOptions;

	}

	public void parseCommonOptions(CommandLine cmd) {


		//We check the options
		//From Main: h,q,b,a,i,n
		//Common: s,w,d,k,r,v,f,m,l,p,g,o,e,c

		// Common Options===========================================================================================
		if (cmd.hasOption('s') || cmd.hasOption("sketchlen")) {
			//Case of sketchlen
			this.setSketchlen(Integer.parseInt(cmd.getOptionValue("sketchlen")));

		}

		if (cmd.hasOption('w') || cmd.hasOption("winlen")) {
			// Case of winlen
			this.setWinlen(Integer.parseInt(cmd.getOptionValue("winlen")));

		}

		if (cmd.hasOption('d') || cmd.hasOption("winstride")) {
			// Case of winstride
			this.setWinstride(Integer.parseInt(cmd.getOptionValue("winstride")));

		}

		if (cmd.hasOption('k') || cmd.hasOption("kmerlen")) {
			// Case of kmerlen
			this.setKmerlen(Integer.parseInt(cmd.getOptionValue("kmerlen")));

		}

		if (cmd.hasOption('r') || cmd.hasOption("max_locations_per_feature")) {
			// Case of max_locations_per_feature
			this.setMax_locations_per_feature(Integer.parseInt(cmd.getOptionValue("max_locations_per_feature")));
		}

		if (cmd.hasOption('v') || cmd.hasOption("verbose")) {
			// Case of verbose
			this.setVerboseValue(true);
		}

		if (cmd.hasOption('f') || cmd.hasOption("max_load_fac")) {
			this.setMax_load_fac(Double.parseDouble(cmd.getOptionValue("max_load_fac")));

		}


		if (cmd.hasOption('m') || cmd.hasOption("maxTargetsPerSketchVal")) {
			this.setMaxTargetsPerSketchVal(Integer.parseInt(cmd.getOptionValue("maxTargetsPerSketchVal")));
		}

		if (cmd.hasOption('l') || cmd.hasOption("num_partitions")) {
			this.setNumPartitions(Integer.parseInt(cmd.getOptionValue("num_partitions")));

		}


		if(cmd.hasOption("p") || cmd.hasOption("hashmap")) {
			this.setBuildModeHashMap(true);
			this.setBuildModeHashMultiMapG(false);
			this.setBuildModeHashMultiMapMC(false);
			this.setBuildModeParquetDataframe(false);
			this.setBuildCombineByKey(false);
		}

		else if(cmd.hasOption("g") || cmd.hasOption("hashmultimap")) {
			this.setBuildModeHashMap(false);
			this.setBuildModeHashMultiMapG(true);
			this.setBuildModeHashMultiMapMC(false);
			this.setBuildModeParquetDataframe(false);
			this.setBuildCombineByKey(false);
		}
		else if(cmd.hasOption("o") || cmd.hasOption("hashmultimapMC")) {
			this.setBuildModeHashMap(false);
			this.setBuildModeHashMultiMapG(false);
			this.setBuildModeHashMultiMapMC(true);
			this.setBuildModeParquetDataframe(false);
			this.setBuildCombineByKey(false);
		}
		else if(cmd.hasOption("e") || cmd.hasOption("dataframe")) {
			this.setBuildModeHashMap(false);
			this.setBuildModeHashMultiMapG(false);
			this.setBuildModeHashMultiMapMC(false);
			this.setBuildModeParquetDataframe(true);
			this.setBuildCombineByKey(false);
		}
		else if(cmd.hasOption("c") || cmd.hasOption("combine")) {
			this.setBuildModeHashMap(false);
			this.setBuildModeHashMultiMapG(false);
			this.setBuildModeHashMultiMapMC(false);
			this.setBuildModeParquetDataframe(false);
			this.setBuildCombineByKey(true);
		}


	}


	public boolean isVerboseValue() {
		return verboseValue;
	}

	public void setVerboseValue(boolean verboseValue) {
		this.verboseValue = verboseValue;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}

	public boolean isBuildModeHashMap() {
		return buildModeHashMap;
	}

	public void setBuildModeHashMap(boolean buildModeHashMap) {
		this.buildModeHashMap = buildModeHashMap;
	}

	public boolean isBuildModeHashMultiMapG() {
		return buildModeHashMultiMapG;
	}

	public void setBuildModeHashMultiMapG(boolean buildModeHashMultiMapG) {
		this.buildModeHashMultiMapG = buildModeHashMultiMapG;
	}

	public boolean isBuildModeHashMultiMapMC() {
		return buildModeHashMultiMapMC;
	}

	public void setBuildModeHashMultiMapMC(boolean buildModeHashMultiMapMC) {
		this.buildModeHashMultiMapMC = buildModeHashMultiMapMC;
	}

	public boolean isBuildModeParquetDataframe() {
		return buildModeParquetDataframe;
	}

	public void setBuildModeParquetDataframe(boolean buildModeParquetDataframe) {
		this.buildModeParquetDataframe = buildModeParquetDataframe;
	}

	public boolean isBuildCombineByKey() {
		return buildCombineByKey;
	}

	public void setBuildCombineByKey(boolean buildCombineByKey) {
		this.buildCombineByKey = buildCombineByKey;
	}

	public int getSketchlen() {
		return sketchlen;
	}

	public void setSketchlen(int sketchlen) {
		this.sketchlen = sketchlen;
	}

	public int getWinlen() {
		return winlen;
	}

	public void setWinlen(int winlen) {
		this.winlen = winlen;
	}

	public int getWinstride() {
		return winstride;
	}

	public void setWinstride(int winstride) {
		this.winstride = winstride;
	}

	public int getKmerlen() {
		return kmerlen;
	}

	public void setKmerlen(int kmerlen) {
		this.kmerlen = kmerlen;
	}

	public double getMax_load_fac() {
		return max_load_fac;
	}

	public void setMax_load_fac(double max_load_fac) {
		this.max_load_fac = max_load_fac;
	}

	public int getMaxTargetsPerSketchVal() {
		return maxTargetsPerSketchVal;
	}

	public void setMaxTargetsPerSketchVal(int maxTargetsPerSketchVal) {
		this.maxTargetsPerSketchVal = maxTargetsPerSketchVal;
	}

	public int getMax_locations_per_feature() {
		return max_locations_per_feature;
	}

	public void setMax_locations_per_feature(int max_locations_per_feature) {
		this.max_locations_per_feature = max_locations_per_feature;
	}

	public Options getOptions() {
		return options;
	}

	public void setOptions(Options options) {
		this.options = options;
	}
}
