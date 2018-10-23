package com.github.jmabuin.metacachespark.options;

import com.github.jmabuin.metacachespark.EnumModes;
import com.github.jmabuin.metacachespark.database.Taxonomy;

import java.util.Properties;

public class MetaCacheProperties {

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

    /*
     * Output options & formatting
     */

    private EnumModes.map_view_mode mapViewMode      = EnumModes.map_view_mode.all;           // How to show classification (read mappings), if 'none', only summary will be shown
    private EnumModes.taxon_print_mode showTaxaAs    = EnumModes.taxon_print_mode.name_only;  //what to show of a taxon

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
    private EnumModes.pairing_mode pairing   = EnumModes.pairing_mode.none;
    private Taxonomy.Rank lowestRank         = Taxonomy.Rank.Sequence;               // Ranks/taxa to classify on and to show
    private Taxonomy.Rank highestRank        = Taxonomy.Rank.Domain;
    private Taxonomy.Rank excludedRank       = Taxonomy.Rank.none;                   // Ground truth rank to exclude (for clade exclusion test)

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

    private String taxpostmapValue 	= "";

    private Taxonomy.Rank removeAmbigFeaturesOnRank = Taxonomy.Rank.none;

    private int maxTaxaPerFeature = 1;

    private boolean myWholeTextFiles = false;

    public MetaCacheProperties(Properties properties) {

        this.sketchlen = Integer.parseInt(properties.getProperty("sketchlen"));
        this.winlen    = Integer.parseInt(properties.getProperty("winlen"));
        this.winstride = Integer.parseInt(properties.getProperty("winstride"));
        this.kmerlen   = Integer.parseInt(properties.getProperty("kmerlen"));

        this.max_load_fac        = Double.parseDouble(properties.getProperty("max_load_fac"));
        this.maxTargetsPerSketchVal  = Integer.parseInt(properties.getProperty("maxTargetsPerSketchVal"));
        this.max_locations_per_feature = Integer.parseInt(properties.getProperty("max_locations_per_feature"));

        System.out.println("Value of max_locations_per_feature is: " + this.max_locations_per_feature);


        this.showDBproperties        = Boolean.parseBoolean("showDBproperties");
        this.splitOutput             = Boolean.parseBoolean("splitOutput");
        this.showTopHits             = Boolean.parseBoolean("showTopHits");
        this.showAllHits             = Boolean.parseBoolean("showAllHits");
        this.showLocations           = Boolean.parseBoolean("showLocations");
        this.showGroundTruth         = Boolean.parseBoolean("showGroundTruth");
        this.showLineage             = Boolean.parseBoolean("showLineage");
        this.showAlignment           = Boolean.parseBoolean("showAlignment");

        this.hitsMin                 = Integer.parseInt(properties.getProperty("hitsMin"));
        this.hitsDiff                = Double.parseDouble(properties.getProperty("hitsDiff"));
        this.insertSizeMax           = Integer.parseInt(properties.getProperty("insertSizeMax"));
        this.weightHitsWithWindows   = Boolean.parseBoolean("weightHitsWithWindows");

        /*
         * Analysis options
         */
        this.testPrecision       = Boolean.parseBoolean("testPrecision");
        this.testCoverage        = Boolean.parseBoolean("testCoverage");
        this.testAlignment       = Boolean.parseBoolean("testAlignment");

        this.sequ2taxonPreFile = properties.getProperty("sequ2taxonPreFile");

        this.taxpostmapValue = properties.getProperty("taxpostmap");

        this.removeAmbigFeaturesOnRank = Taxonomy.rank_from_name(properties.getProperty("remove_ambig_features"));

        this.maxTaxaPerFeature = Integer.parseInt(properties.getProperty("max_ambig_per_feature"));

        this.myWholeTextFiles = Boolean.parseBoolean(properties.getProperty("wholetextfiles"));

    }

    public int getSketchlen() {
        return sketchlen;
    }

    public int getWinlen() {
        return winlen;
    }

    public int getWinstride() {
        return winstride;
    }

    public int getKmerlen() {
        return kmerlen;
    }

    public double getMax_load_fac() {
        return max_load_fac;
    }

    public int getMaxTargetsPerSketchVal() {
        return maxTargetsPerSketchVal;
    }

    public int getMax_locations_per_feature() {
        return max_locations_per_feature;
    }

    public boolean isVerboseValue() {
        return verboseValue;
    }

    public EnumModes.map_view_mode getMapViewMode() {
        return mapViewMode;
    }

    public EnumModes.taxon_print_mode getShowTaxaAs() {
        return showTaxaAs;
    }

    public boolean isShowDBproperties() {
        return showDBproperties;
    }

    public boolean isSplitOutput() {
        return splitOutput;
    }

    public boolean isShowTopHits() {
        return showTopHits;
    }

    public boolean isShowAllHits() {
        return showAllHits;
    }

    public boolean isShowLocations() {
        return showLocations;
    }

    public boolean isShowGroundTruth() {
        return showGroundTruth;
    }

    public boolean isShowLineage() {
        return showLineage;
    }

    public boolean isShowAlignment() {
        return showAlignment;
    }

    public String getComment() {
        return comment;
    }

    public String getOutSeparator() {
        return outSeparator;
    }

    public EnumModes.pairing_mode getPairing() {
        return pairing;
    }

    public Taxonomy.Rank getLowestRank() {
        return lowestRank;
    }

    public Taxonomy.Rank getHighestRank() {
        return highestRank;
    }

    public Taxonomy.Rank getExcludedRank() {
        return excludedRank;
    }

    public int getHitsMin() {
        return hitsMin;
    }

    public double getHitsDiff() {
        return hitsDiff;
    }

    public int getInsertSizeMax() {
        return insertSizeMax;
    }

    public boolean isWeightHitsWithWindows() {
        return weightHitsWithWindows;
    }

    public boolean isTestPrecision() {
        return testPrecision;
    }

    public boolean isTestCoverage() {
        return testCoverage;
    }

    public boolean isTestAlignment() {
        return testAlignment;
    }

    public String getSequ2taxonPreFile() {
        return sequ2taxonPreFile;
    }

    public String getTaxpostmapValue() {
        return taxpostmapValue;
    }

    public Taxonomy.Rank getRemoveAmbigFeaturesOnRank() {
        return removeAmbigFeaturesOnRank;
    }

    public int getMaxTaxaPerFeature() {
        return maxTaxaPerFeature;
    }

    public boolean isMyWholeTextFiles() {
        return myWholeTextFiles;
    }

    public void setHitsMin(int hitsMin) {
        this.hitsMin = hitsMin;
    }
}