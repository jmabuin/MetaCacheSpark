/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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

package com.github.metacachespark;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

public class MetaCacheOptions implements Serializable {

	private static final Log LOG = LogFactory.getLog(MetaCacheOptions.class);

	private Options options = null;

	public enum Mode { HELP, QUERY, BUILD, ADD, INFO, ANNOTATE}

	private Mode mode;

	private String correctUse =
			"spark-submit --class com.github.metachachespark.MetaCacheSpark MetaCacheSpark-0.0.1.jar";// [SparkBWA Options] Input.fastq [Input2.fastq] Output\n";


	// Header to show when the program is not launched correctly
	private String header = "\tMetaCacheSpark performs metagenomics ...\nAvailable operating modes are:\n";

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

		this.mode = Mode.HELP;

		try {
			cmd = parser.parse(this.options, args, true);

			//We look for the operation mode
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
			} else {
				// Default case. Help
				LOG.warn("[" + this.getClass().getName() + "] :: No operation mode selected. Using help ");
			}

			// Get and parse the rest of the arguments
			this.otherOptions = cmd.getArgs(); //With this we get the rest of the arguments

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

		OptionGroup modes = new OptionGroup();

		Option help = new Option("h","help", false,"Shows documentation");
		modes.addOption(help);

		Option query = new Option("q","query", false,"Classify read sequences using pre-built database");
		modes.addOption(query);

		Option build = new Option("b", "build", false, "Build new database from reference genomes");
		modes.addOption(build);

		Option add = new Option("a", "add", false, "Add reference genomes and/or taxonomy to existing database");
		modes.addOption(add);

		Option info = new Option("i", "info", false, "Shows database and reference genome properties");
		modes.addOption(info);

		Option annotate = new Option("n", "annotate", false, "Annotate sequences with taxonomic information");
		modes.addOption(annotate);

		privateOptions.addOptionGroup(modes);

		return privateOptions;
	}

	public Mode getMode() {
		return this.mode;
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
}
