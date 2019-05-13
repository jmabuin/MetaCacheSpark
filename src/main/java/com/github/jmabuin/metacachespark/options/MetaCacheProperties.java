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

import java.io.Serializable;
import java.util.Properties;

public class MetaCacheProperties implements Serializable {

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
    private double hitsDiffFraction         = 1.0;
    private int insertSizeMax               = 0;        // Maximum range in sequence that read (pair) is expected to be in
    private boolean weightHitsWithWindows   = false;
    private Taxonomy.Rank mergeBelow        = Taxonomy.rank_from_name("sequence");
    /*
     * Analysis options
     */
    private boolean testPrecision       = false;    // Test precision (ground truth must be available)
    private boolean testCoverage        = false;
    private boolean testAlignment       = false;

    private String sequ2taxonPreFile;               // Additional file with query -> ground truth mapping

    private long maxCandidates          = Long.MAX_VALUE;

    private String taxpostmapValue 	= "";

    private Taxonomy.Rank removeAmbigFeaturesOnRank = Taxonomy.Rank.none;

    private int maxTaxaPerFeature = 1;

    //private boolean myWholeTextFiles = false;

    public MetaCacheProperties(Properties properties) {

        this.sketchlen = Integer.parseInt(properties.getProperty("sketchlen"));
        this.winlen    = Integer.parseInt(properties.getProperty("winlen"));
        this.winstride = Integer.parseInt(properties.getProperty("winstride"));
        this.kmerlen   = Integer.parseInt(properties.getProperty("kmerlen"));

        this.max_load_fac        = Double.parseDouble(properties.getProperty("max_load_fac"));
        this.maxTargetsPerSketchVal  = Integer.parseInt(properties.getProperty("maxTargetsPerSketchVal"));
        this.max_locations_per_feature = Integer.parseInt(properties.getProperty("max_locations_per_feature"));

        this.showDBproperties        = Boolean.parseBoolean(properties.getProperty("showDBproperties"));
        this.splitOutput             = Boolean.parseBoolean(properties.getProperty("splitOutput"));
        this.showTopHits             = Boolean.parseBoolean(properties.getProperty("showTopHits"));
        this.showAllHits             = Boolean.parseBoolean(properties.getProperty("showAllHits"));
        this.showLocations           = Boolean.parseBoolean(properties.getProperty("showLocations"));
        this.showGroundTruth         = Boolean.parseBoolean(properties.getProperty("showGroundTruth"));
        this.showLineage             = Boolean.parseBoolean(properties.getProperty("showLineage"));
        this.showAlignment           = Boolean.parseBoolean(properties.getProperty("showAlignment"));

        this.hitsMin                 = Integer.parseInt(properties.getProperty("hitsMin"));
        this.hitsDiff                = Double.parseDouble(properties.getProperty("hitsDiff"));
        this.hitsDiffFraction        = Double.parseDouble(properties.getProperty("hitsDiffFraction"));

        if(this.hitsDiffFraction > 1.0) {
            this.hitsDiffFraction *= 0.01;
        }

        this.insertSizeMax           = Integer.parseInt(properties.getProperty("insertSizeMax"));
        this.weightHitsWithWindows   = Boolean.parseBoolean("weightHitsWithWindows");
        this.mergeBelow              = Taxonomy.rank_from_name(properties.getProperty("mergeBelow"));
        /*
         * Analysis options
         */
        this.testPrecision       = Boolean.parseBoolean(properties.getProperty("testPrecision"));
        this.testCoverage        = Boolean.parseBoolean(properties.getProperty("testCoverage"));
        this.testAlignment       = Boolean.parseBoolean(properties.getProperty("testAlignment"));

        this.sequ2taxonPreFile = properties.getProperty("sequ2taxonPreFile");

        this.taxpostmapValue = properties.getProperty("taxpostmap");

        this.maxCandidates = Long.parseLong(properties.getProperty("maxCandidates"));

        this.removeAmbigFeaturesOnRank = Taxonomy.rank_from_name(properties.getProperty("remove_ambig_features"));

        this.maxTaxaPerFeature = Integer.parseInt(properties.getProperty("max_ambig_per_feature"));

        if (this.mergeBelow != Taxonomy.Rank.Sequence) {
            this.lowestRank = this.mergeBelow;
        }
        //this.myWholeTextFiles = Boolean.parseBoolean(properties.getProperty("wholetextfiles"));

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

    /*
    public boolean isMyWholeTextFiles() {
        return myWholeTextFiles;
    }
    */
    public void setHitsMin(int hitsMin) {
        this.hitsMin = hitsMin;
    }

    public double getHitsDiffFraction() {
        return hitsDiffFraction;
    }

    public long getMaxCandidates() {
        return maxCandidates;
    }

    public void setMaxCandidates(long maxCandidates) {
        this.maxCandidates = maxCandidates;
    }

    public Taxonomy.Rank getMergeBelow() {
        return mergeBelow;
    }
}
