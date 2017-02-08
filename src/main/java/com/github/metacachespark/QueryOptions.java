package com.github.metacachespark;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by jabuinmo on 07.02.17.
 */
public class QueryOptions implements Serializable {
    private static final Log LOG = LogFactory.getLog(QueryOptions.class);

    private Options options = null;

    /*
     * Output options & formatting
     */

    private MetaCacheOptions.map_view_mode mapViewMode      = MetaCacheOptions.map_view_mode.all;           // How to show classification (read mappings), if 'none', only summary will be shown
    private MetaCacheOptions.taxon_print_mode showTaxaAs    = MetaCacheOptions.taxon_print_mode.name_only;  //what to show of a taxon

    private boolean showDBproperties        = false;       // Show database properties
    private boolean splitOutput             = false;       // Make a separate output file for each input file
    private boolean showTopHits             = false;       // Show top candidate sequences and their associated k-mer hash hit count
    private boolean showAllHits             = false;       // Show all k-mer-hash hits in database for each given read
    private boolean showLocations           = false;       // Show candidate position(s) in reference sequence(s)
    private boolean showGroundTruth         = false;       // Show known taxon (or complete lineage if 'showLineage' on)
    private boolean showLineage             = false;       // Show all ranks that a sequence could be classified on
    private boolean showAlignment           = false;
    private String comment                  = "# ";        // Prefix for each non-mapping line
    private String outSeparator             = "\t|\t";     // Separates individual mapping fields

    /*
     * Classification options
     */
    private MetaCacheOptions.pairing_mode pairing   = MetaCacheOptions.pairing_mode.none;
    private Taxonomy.Rank lowestRank                = Taxonomy.Rank.Sequence;               // Ranks/taxa to classify on and to show
    private Taxonomy.Rank highestRank               = Taxonomy.Rank.Domain;
    private Taxonomy.Rank excludedRank              = Taxonomy.Rank.none;                   // Ground truth rank to exclude (for clade exclusion test)

    private int hitsMin                     = 0;        // < 1 : deduced from database parameters
    private double hitsDiff                 = 0.5;
    private int insertSizeMax               = 0;        // Maximum range in sequence that read (pair) is expected to be in
    private boolean weightHitsWithWindows   = false;

    /*
     * Analysis options
     */
    private boolean testPrecision       = false;    // Test precision (ground truth must be available)
    private boolean testCoverage        = false;
    private boolean testAlignment       = false;

    private String sequ2taxonPreFile;               // Additional file with query -> ground truth mapping

    /*
     * Query sampling scheme
     */
    private int sketchlen = -1;  //< 0 : use value from database
    private int winlen    = -1;  //< 0 : use value from database
    private int winstride = -1;  //< 0 : use value from database

    /*
     * Tuning parameters
     */
    private double maxLoadFactor        = -1; //< 0 : use value from database
    private int maxTargetsPerSketchVal  = -1; //< 0 : use value from database
    private int numThreads              = 1;

    /*
     * Filenames
     */
    private String dbfile;
    //std::vector<std::string> infiles;
    private String[] infiles;
    private String outfile;

    private String[] otherOptions;

    public QueryOptions(String args[]) {

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
                this.sketchlen = Integer.parseInt(cmd.getOptionValue("sketchlen"));

            }
            if (cmd.hasOption('w') || cmd.hasOption("winlen")) {
                // Case of winlen
                this.winlen = Integer.parseInt(cmd.getOptionValue("winlen"));

            }

            if (cmd.hasOption('d') || cmd.hasOption("winstride")) {
                // Case of winstride
                this.winstride = Integer.parseInt(cmd.getOptionValue("winstride"));
            }

            if (cmd.hasOption('p') || cmd.hasOption("showDBproperties")) {
                this.showDBproperties = true;
            }

            if(cmd.hasOption("pairedFiles") || cmd.hasOption("P")) {
                this.pairing = MetaCacheOptions.pairing_mode.files;
            }
            else if(cmd.hasOption("pairedSequences") || cmd.hasOption("S")) {
                this.pairing = MetaCacheOptions.pairing_mode.sequences;
            }

            if (cmd.hasOption('c') || cmd.hasOption("coverage")) {
                this.testCoverage = true;
            }

            if (cmd.hasOption('r') || cmd.hasOption("precision")) {
                this.testPrecision = true;
            }

            this.testPrecision = this.testCoverage || this.testPrecision;

            if (cmd.hasOption('l') || cmd.hasOption("showLocations")) {
                this.showLocations = true;
            }

            if (cmd.hasOption('t') || cmd.hasOption("showTopHits")) {
                this.showTopHits = true;
            }

            if (cmd.hasOption('a') || cmd.hasOption("showAllHits")) {
                this.showAllHits = true;
            }

            if (cmd.hasOption('i') || cmd.hasOption("taxids_only")) {
                this.showTaxaAs = MetaCacheOptions.taxon_print_mode.id_only;
            }
            else if (cmd.hasOption('j') || cmd.hasOption("taxid")) {
                this.showTaxaAs = MetaCacheOptions.taxon_print_mode.id_name;
            }
            else if (cmd.hasOption('n') || cmd.hasOption("name_only")) {
                this.showTaxaAs = MetaCacheOptions.taxon_print_mode.name_only;
            }

            if (cmd.hasOption('b') || cmd.hasOption("nomap")) {
                this.mapViewMode = MetaCacheOptions.map_view_mode.none;
            }
            else if (cmd.hasOption('c') || cmd.hasOption("mappedOnly")) {
                this.mapViewMode = MetaCacheOptions.map_view_mode.mapped_only;
            }

            //showing hits changes the mapping mode!
            if(this.mapViewMode == MetaCacheOptions.map_view_mode.none && this.showTopHits) {
                this.mapViewMode = MetaCacheOptions.map_view_mode.mapped_only;
            }
            else if(this.showAllHits) {
                this.mapViewMode =MetaCacheOptions.map_view_mode.all;
            }

            if (cmd.hasOption('j') || cmd.hasOption("showGroundTruth")) {
                this.showGroundTruth = true;
            }

            if (cmd.hasOption('e') || cmd.hasOption("insertSizeMax")) {
                this.insertSizeMax = Integer.parseInt(cmd.getOptionValue("insertSizeMax"));
            }

            if (cmd.hasOption('f') || cmd.hasOption("max_load_fac")) {
                this.maxLoadFactor = Double.parseDouble(cmd.getOptionValue("max_load_fac"));
            }

            if (cmd.hasOption('g') || cmd.hasOption("max_locations_per_feature")) {
                this.maxTargetsPerSketchVal = Integer.parseInt(cmd.getOptionValue("maxTargetsPerSketchVal"));
            }

            if (cmd.hasOption('h') || cmd.hasOption("threads")) {
                this.numThreads = Integer.parseInt(cmd.getOptionValue("numThreads"));
            }

            // Get and parse the rest of the arguments
            this.otherOptions = cmd.getArgs(); //With this we get the rest of the arguments

            // Check if the number of arguments is correct. This is, dbname, outfile and infiles
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
                //this.infiles 	= this.otherOptions[2:this.otherOptions.length];
                this.infiles = Arrays.copyOfRange(this.otherOptions, 2, this.otherOptions.length);

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

        Option showDBproperties = new Option("p","showDBproperties", false,"Show database properties");
        privateOptions.addOption(showDBproperties);

        OptionGroup pairing = new OptionGroup();
        Option pairedFiles = new Option("P","pairedFiles", false, "Paired files");
        pairing.addOption(pairedFiles);

        Option pairedSequences = new Option("S", "pairedSequences", false,"Paired sequences");
        pairing.addOption(pairedSequences);

        privateOptions.addOptionGroup(pairing);

        Option coverage = new Option("c", "coverage", false,"Test precision coverage");
        privateOptions.addOption(coverage);

        Option precision = new Option("r", "precision", false, "Test precision");
        privateOptions.addOption(precision);

        Option showLocations = new Option("l", "showLocations", false, "Show candidate position(s) in reference sequence(s)");
        privateOptions.addOption(showLocations);

        Option showTopHits = new Option("t", "showTopHits", false, "Show top candidate sequences and their associated k-mer hash hit count");
        privateOptions.addOption(showTopHits);

        Option showAllHits = new Option("a", "showAllHits", false, "Show all k-mer-hash hits in database for each given read");
        privateOptions.addOption(showAllHits);

        OptionGroup taxonPrintMode = new OptionGroup();
        Option taxids_only = new Option("i", "taxids_only", false, "Only tax ids");
        taxonPrintMode.addOption(taxids_only);

        Option taxid = new Option("j", "taxid", false,"Tax ids and name");
        taxonPrintMode.addOption(taxid);

        Option name_only = new Option("n", "name_only", false, "Name only");
        taxonPrintMode.addOption(name_only);

        privateOptions.addOptionGroup(taxonPrintMode);

        OptionGroup mapViewMode = new OptionGroup();
        Option nomap = new Option("b", "nomap", false, "Show only classification summary");
        mapViewMode.addOption(nomap);

        Option mappedOnly = new Option("c", "mappedOnly", false,"Show mappings in classification");
        mapViewMode.addOption(mappedOnly);
        privateOptions.addOptionGroup(mapViewMode);

        Option showGroundTruth = new Option("j", "showGroundTruth", false, "Show known taxon (or complete lineage if 'showLineage' on)");
        privateOptions.addOption(showGroundTruth);

        Option insertSizeMax = new Option("e", "insertSizeMax", true, "Maximum range in sequence that read (pair) is expected to be in");
        privateOptions.addOption(insertSizeMax);

        Option max_load_fac = new Option("f", "max_load_fac", true,"Maximum value for load factor");
        //buildOptions.addOption(max_load_fac);
        privateOptions.addOption(max_load_fac);

        Option maxTargetsPerSketchVal = new Option("g", "max_locations_per_feature", true, "Maximum number of locations per feature");
        privateOptions.addOption(maxTargetsPerSketchVal);

        Option numThreads = new Option("i", "threads", true, "Number of threads to use");
        privateOptions.addOption(numThreads);

        return privateOptions;

    }

    public MetaCacheOptions.map_view_mode getMapViewMode() {
        return mapViewMode;
    }

    public void setMapViewMode(MetaCacheOptions.map_view_mode mapViewMode) {
        this.mapViewMode = mapViewMode;
    }

    public MetaCacheOptions.taxon_print_mode getShowTaxaAs() {
        return showTaxaAs;
    }

    public void setShowTaxaAs(MetaCacheOptions.taxon_print_mode showTaxaAs) {
        this.showTaxaAs = showTaxaAs;
    }

    public boolean isShowDBproperties() {
        return showDBproperties;
    }

    public void setShowDBproperties(boolean showDBproperties) {
        this.showDBproperties = showDBproperties;
    }

    public boolean isSplitOutput() {
        return splitOutput;
    }

    public void setSplitOutput(boolean splitOutput) {
        this.splitOutput = splitOutput;
    }

    public boolean isShowTopHits() {
        return showTopHits;
    }

    public void setShowTopHits(boolean showTopHits) {
        this.showTopHits = showTopHits;
    }

    public boolean isShowAllHits() {
        return showAllHits;
    }

    public void setShowAllHits(boolean showAllHits) {
        this.showAllHits = showAllHits;
    }

    public boolean isShowLocations() {
        return showLocations;
    }

    public void setShowLocations(boolean showLocations) {
        this.showLocations = showLocations;
    }

    public boolean isShowGroundTruth() {
        return showGroundTruth;
    }

    public void setShowGroundTruth(boolean showGroundTruth) {
        this.showGroundTruth = showGroundTruth;
    }

    public boolean isShowLineage() {
        return showLineage;
    }

    public void setShowLineage(boolean showLineage) {
        this.showLineage = showLineage;
    }

    public boolean isShowAlignment() {
        return showAlignment;
    }

    public void setShowAlignment(boolean showAlignment) {
        this.showAlignment = showAlignment;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getOutSeparator() {
        return outSeparator;
    }

    public void setOutSeparator(String outSeparator) {
        this.outSeparator = outSeparator;
    }

    public MetaCacheOptions.pairing_mode getPairing() {
        return pairing;
    }

    public void setPairing(MetaCacheOptions.pairing_mode pairing) {
        this.pairing = pairing;
    }

    public Taxonomy.Rank getLowestRank() {
        return lowestRank;
    }

    public void setLowestRank(Taxonomy.Rank lowestRank) {
        this.lowestRank = lowestRank;
    }

    public Taxonomy.Rank getHighestRank() {
        return highestRank;
    }

    public void setHighestRank(Taxonomy.Rank highestRank) {
        this.highestRank = highestRank;
    }

    public Taxonomy.Rank getExcludedRank() {
        return excludedRank;
    }

    public void setExcludedRank(Taxonomy.Rank excludedRank) {
        this.excludedRank = excludedRank;
    }

    public int getHitsMin() {
        return hitsMin;
    }

    public void setHitsMin(int hitsMin) {
        this.hitsMin = hitsMin;
    }

    public double getHitsDiff() {
        return hitsDiff;
    }

    public void setHitsDiff(double hitsDiff) {
        this.hitsDiff = hitsDiff;
    }

    public int getInsertSizeMax() {
        return insertSizeMax;
    }

    public void setInsertSizeMax(int insertSizeMax) {
        this.insertSizeMax = insertSizeMax;
    }

    public boolean isWeightHitsWithWindows() {
        return weightHitsWithWindows;
    }

    public void setWeightHitsWithWindows(boolean weightHitsWithWindows) {
        this.weightHitsWithWindows = weightHitsWithWindows;
    }

    public boolean isTestPrecision() {
        return testPrecision;
    }

    public void setTestPrecision(boolean testPrecision) {
        this.testPrecision = testPrecision;
    }

    public boolean isTestCoverage() {
        return testCoverage;
    }

    public void setTestCoverage(boolean testCoverage) {
        this.testCoverage = testCoverage;
    }

    public boolean isTestAlignment() {
        return testAlignment;
    }

    public void setTestAlignment(boolean testAlignment) {
        this.testAlignment = testAlignment;
    }

    public String getSequ2taxonPreFile() {
        return sequ2taxonPreFile;
    }

    public void setSequ2taxonPreFile(String sequ2taxonPreFile) {
        this.sequ2taxonPreFile = sequ2taxonPreFile;
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

    public double getMaxLoadFactor() {
        return maxLoadFactor;
    }

    public void setMaxLoadFactor(double maxLoadFactor) {
        this.maxLoadFactor = maxLoadFactor;
    }

    public int getMaxTargetsPerSketchVal() {
        return maxTargetsPerSketchVal;
    }

    public void setMaxTargetsPerSketchVal(int maxTargetsPerSketchVal) {
        this.maxTargetsPerSketchVal = maxTargetsPerSketchVal;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public String getDbfile() {
        return dbfile;
    }

    public void setDbfile(String dbfile) {
        this.dbfile = dbfile;
    }

    public String[] getInfiles() {
        return infiles;
    }

    public void setInfiles(String[] infiles) {
        this.infiles = infiles;
    }

    public String getOutfile() {
        return outfile;
    }

    public void setOutfile(String outfile) {
        this.outfile = outfile;
    }
}
