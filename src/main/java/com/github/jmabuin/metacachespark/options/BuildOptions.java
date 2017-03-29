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
public class BuildOptions extends CommonOptions implements Serializable {

	private static final Log LOG = LogFactory.getLog(BuildOptions.class);

	private Options buildOptions = null;

	// Default options values


	private String taxonomyValue 	= "";
	private String taxpostmapValue 	= "";

	private Taxonomy.Rank removeAmbigFeaturesOnRank = Taxonomy.Rank.none;

	private int maxTaxaPerFeature = 1;

	private String dbfile			= "";
	private String infiles			= "";

	private TaxonomyParam taxonomyParam;

	private boolean myWholeTextFiles = false;

	private String[] otherOptions;


	public BuildOptions(String args[]) {

		super();

		this.buildOptions = this.getOptions();//this.initOptions(newArgs);

		this.initOptions();

		//Parse the given arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;

		try {
			cmd = parser.parse(this.buildOptions, args);

			// Common options
			this.parseCommonOptions(cmd);

			//From Main: h,q,b,a,i,n
			//From Common: s,w,d,k,r,v,f,m,l,p,g,o,e,c
			// Build Options============================================================================================
			if (cmd.hasOption('t') || cmd.hasOption("taxonomy")) {
				// Case of taxonomy
				this.taxonomyValue = cmd.getOptionValue("taxonomy");

			}

			if (cmd.hasOption('y') || cmd.hasOption("taxpostmap")) {
				// Case of taxpostmap
				this.taxpostmapValue = cmd.getOptionValue("taxpostmap");

			}


			if (cmd.hasOption('z') || cmd.hasOption("remove_ambig_features")) {
				// Case of max_load_fac
				this.removeAmbigFeaturesOnRank = Taxonomy.rank_from_name(cmd.getOptionValue("remove_ambig_features"));

			}

			if (cmd.hasOption('x') || cmd.hasOption("max_ambig_per_feature")) {
				// Case of max_load_fac
				this.maxTaxaPerFeature = Integer.parseInt("max_ambig_per_feature");

			}

			if (cmd.hasOption('j') || cmd.hasOption("wholetextfiles")) {
				this.myWholeTextFiles = true;

			}


			/*
			if(this.taxpostmapValue != "" && this.taxonomyValue != "") {
				this.taxonomyParam = new TaxonomyParam(this.taxonomyValue, this.taxpostmapValue);
			}
			*/
			if(this.taxonomyValue != "") {
				this.taxonomyParam = new TaxonomyParam(this.taxonomyValue, this.taxpostmapValue);
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


	private void initOptions() {
		//Options privateOptions = new Options();

		Option taxonomy = new Option("t", "taxonomy", true, "Annotate sequences with taxonomic information");
		//buildOptions.addOption(taxonomy);
		this.buildOptions.addOption(taxonomy);

		Option taxpostmap = new Option("y", "taxpostmap", true, "Shows database and reference genome properties");
		//buildOptions.addOption(taxpostmap);
		this.buildOptions.addOption(taxpostmap);

		Option remove_ambig_features = new Option("z", "remove_ambig_features", true, "Remove ambiguous features on rank");
		//buildOptions.addOption(remove_ambig_features);
		this.buildOptions.addOption(remove_ambig_features);

		Option max_ambig_per_feature = new Option("x","max_ambig_per_feature", true, "Maximum ambiguous per feature" );
		//buildOptions.addOption(max_ambig_per_feature);
		this.buildOptions.addOption(max_ambig_per_feature);

		Option wholetextfiles = new Option("j", "wholetextfiles", false, "Use own implementation of wholetextfiles");
		this.buildOptions.addOption(wholetextfiles);

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

	public boolean isMyWholeTextFiles() {
		return myWholeTextFiles;
	}

	public void setMyWholeTextFiles(boolean myWholeTextFiles) {
		this.myWholeTextFiles = myWholeTextFiles;
	}
}
