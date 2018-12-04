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

package com.github.jmabuin.metacachespark.database;
import com.github.jmabuin.metacachespark.*;
import com.github.jmabuin.metacachespark.io.*;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import com.github.jmabuin.metacachespark.spark.*;
import com.google.common.collect.HashMultimap;
//import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/**
 * Main class for the Database construction and storage in HDFS (Build mode), and also to load it from HDFS (Query mode)
 * @author Jose M. Abuin
 */
public class Database implements Serializable{

    private static final Log LOG = LogFactory.getLog(Database.class);

    private Random urbg_;
    private Sketcher targetSketcher_;
    private Sketcher querySketcher_;
    private long targetWindowSize_;
    private long targetWindowStride_;
    private long queryWindowSize_;
    private long queryWindowStride_;
    private long maxLocsPerFeature_;
    private int nextTargetId_;
    private List<TargetProperty> targets_;
    private Dataset<Location> features_;
    private Dataset<Location> featuresDataframe_;
    private Dataset<Locations> featuresDataset;
    //private DataFrame targetPropertiesDataframe_;
    private JavaRDD<TargetProperty> targetPropertiesJavaRDD;
    private HashMap<String,Integer> sid2gid_;
    private TreeMap<String, Integer> name2tax_;
    private Taxonomy taxa_;
    private TaxonomyParam taxonomyParam;
    private int numPartitions = 1;
    private String dbfile;

    private JavaRDD<Sequence> inputSequences;

    private MetaCacheOptions params;
    //private IndexedRDD<Integer, LocationBasic> indexedRDD;
    private JavaPairRDD<Integer, LocationBasic> locationJavaPairRDD;
    private JavaPairRDD<Integer, List<LocationBasic>> locationJavaPairListRDD;
    private JavaPairRDD<Integer, Iterable<LocationBasic>> locationJavaPairIterableRDD;
    private JavaRDD<Locations> locationsRDD;
    private JavaRDD<HashMultiMapNative> locationJavaRDDHashMultiMapNative;
    private JavaRDD<Location> locationJavaRDD;
    private JavaRDD<HashMap<Integer, List<LocationBasic>>> locationJavaHashRDD;
    private JavaRDD<HashMultimap<Integer, LocationBasic>> locationJavaHashMMRDD;
    private RangePartitioner<Integer, LocationBasic> partitioner;
    private MyCustomPartitioner myPartitioner;

    private JavaSparkContext jsc;
    private SQLContext sqlContext;

    public Database(Sketcher targetSketcher_, Sketcher querySketcher_, long targetWindowSize_, long targetWindowStride_,
                    long queryWindowSize_, long queryWindowStride_, long maxLocsPerFeature_, short nextTargetId_,
                    ArrayList<TargetProperty> targets_, Dataset<Location> features_, TreeMap<String, Integer> name2tax_,
                    Taxonomy taxa_, JavaSparkContext jsc, int numPartitions) {

        this.targetSketcher_ = targetSketcher_;
        this.querySketcher_ = querySketcher_;
        this.targetWindowSize_ = targetWindowSize_;
        this.targetWindowStride_ = targetWindowStride_;
        this.queryWindowSize_ = queryWindowSize_;
        this.queryWindowStride_ = queryWindowStride_;
        this.maxLocsPerFeature_ = maxLocsPerFeature_;
        this.nextTargetId_ = nextTargetId_;
        this.targets_ = targets_;
        this.features_ = features_;
        this.name2tax_ = name2tax_;
        this.taxa_ = taxa_;

        this.jsc = jsc;
        this.numPartitions = numPartitions;
    }


    public Database(JavaSparkContext jsc, TaxonomyParam taxonomyParam, MetaCacheOptions params, int numPartitions, String dbfile) {
        this.jsc = jsc;
        this.taxonomyParam = taxonomyParam;
        this.numPartitions = numPartitions;
        this.dbfile = dbfile;

        this.sqlContext = new SQLContext(this.jsc);

        this.targets_ = new ArrayList<TargetProperty>();
        this.name2tax_ = new TreeMap<String,Integer>();
        //this.sid2gid_ = new HashMap<String, Integer>();

        this.params = params;

    }

    /**
     * @brief Builder to read database from HDFS
     * @param jsc
     * @param dbFile
     * @param params
     */
    public Database(JavaSparkContext jsc, String dbFile, MetaCacheOptions params) {

        LOG.info("Loading database from HDFS...");
        this.jsc = jsc;
        this.dbfile = dbFile;

        this.sqlContext = new SQLContext(this.jsc);

        this.params = params;

        this.queryWindowSize_ = params.getProperties().getWinlen();
        this.queryWindowStride_ = params.getProperties().getWinstride();
        this.targetWindowStride_ = params.getProperties().getWinstride();

        this.numPartitions = this.params.getPartitions();


/*
    if(param.showDBproperties) {
        print_properties(db);
        std::cout << '\n';
    }
*/


        this.loadFromFile();
        this.readTaxonomy();
        this.readTargets();
        //this.readSid2gid();
        this.readName2tax();

        this.nextTargetId_ = this.name2tax_.size();
        this.apply_taxonomy();

    }

    public Random getUrbg_() {
        return urbg_;
    }

    public void setUrbg_(Random urbg_) {
        this.urbg_ = urbg_;
    }

    public Sketcher getTargetSketcher_() {
        return targetSketcher_;
    }

    public void setTargetSketcher_(Sketcher targetSketcher_) {
        this.targetSketcher_ = targetSketcher_;
    }

    public Sketcher getQuerySketcher_() {
        return querySketcher_;
    }

    public void setQuerySketcher_(Sketcher querySketcher_) {
        this.querySketcher_ = querySketcher_;
    }

    public long getTargetWindowSize_() {
        return targetWindowSize_;
    }

    public void setTargetWindowSize_(long targetWindowSize_) {
        this.targetWindowSize_ = targetWindowSize_;
    }

    public long getTargetWindowStride_() {
        return targetWindowStride_;
    }

    public void setTargetWindowStride_(long targetWindowStride_) {
        this.targetWindowStride_ = targetWindowStride_;
    }

    public long getQueryWindowSize_() {
        return queryWindowSize_;
    }

    public void setQueryWindowSize_(long queryWindowSize_) {
        this.queryWindowSize_ = queryWindowSize_;
    }

    public long getQueryWindowStride_() {
        return queryWindowStride_;
    }

    public void setQueryWindowStride_(long queryWindowStride_) {
        this.queryWindowStride_ = queryWindowStride_;
    }

    public long getMaxLocsPerFeature_() {
        return maxLocsPerFeature_;
    }

    public void setMaxLocsPerFeature_(long maxLocsPerFeature_) {
        this.maxLocsPerFeature_ = maxLocsPerFeature_;
    }

    public int getNextTargetId_() {
        return nextTargetId_;
    }

    public void setNextTargetId_(int nextTargetId_) {
        this.nextTargetId_ = nextTargetId_;
    }

    public List<TargetProperty> getTargets_() {
        return targets_;
    }

    public void setTargets_(List<TargetProperty> targets_) {
        this.targets_ = targets_;
    }

    public Dataset<Location> getFeatures_() {
        return features_;
    }

    public void setFeatures_(Dataset<Location> features_) {
        this.features_ = features_;
    }

    public TreeMap<String, Integer> getname2tax_() {
        return name2tax_;
    }

    public void setname2tax_(TreeMap<String, Integer> name2tax_) {
        this.name2tax_ = name2tax_;
    }

    public Taxonomy getTaxa_() {
        return taxa_;
    }

    public void setTaxa_(Taxonomy taxa_) {
        this.taxa_ = taxa_;
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

    public String getDbfile() {
        return dbfile;
    }

    public void setDbfile(String dbfile) {
        this.dbfile = dbfile;
    }

    public JavaSparkContext getSparkS() {
        return jsc;
    }

    public void setSparkS(JavaSparkContext sparkS) {
        this.jsc = sparkS;
    }

	/*
		const taxon&
		taxon_of_target(target_id id) const noexcept {
			return taxa_[targets_[id].taxonId];
		}
		 */

    /**
     * Given an ID, it returns
     * @param id
     * @return
     */
    public Taxon taxon_of_target(Long id) {

        try {
            return taxa_.getTaxa_().get(targets_.get((int) id.longValue()).getTax());
        }
        catch(Exception e) {
            LOG.error("Error in taxon_of_target: "+e.getMessage());
            LOG.error("Error in taxon_of_target: id is: " + id + ", n. of targets is: "+
                    targets_.size()+", n ot taxa is: "+taxa_.getTaxa_().size());
            System.exit(-1);
        }
        return null;
    }

    public long taxon_id_of_target(Long id) {
        return taxa_.getTaxa_().get(targets_.get((int)id.longValue()).getTax()).getTaxonId();
    }

    public void update_lineages(TargetProperty gp)
    {
        if(gp.getTax() > 0) {
            //LOG.warn("Updating target with tax: "+gp.getTax());
            gp.setFull_lineage(taxa_.lineage(gp.getTax()));
            gp.setRanked_lineage(taxa_.ranks((Long)gp.getTax()));
        }
    }

    public void apply_taxonomy(Taxonomy tax) {
        this.taxa_ = tax;

        /*for(Map.Entry<Long, Taxon> taxi : this.taxa_.getTaxa_().entrySet()){
            if (taxi.getKey() == 0){
                LOG.info(taxi.getKey() + " :: " + taxi.getValue().getTaxonName());
            }

        }*/

        for(TargetProperty g : this.targets_)
            update_lineages(g);
    }

    public void apply_taxonomy() {
        for(TargetProperty g : this.targets_)
            update_lineages(g);
    }

    public long taxon_count() {
        return this.taxa_.taxon_count();
    }

    public long target_count() {
        return this.targets_.size();
    }


    public void buildDatabase2(String infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {
            LOG.warn("[buildDatabase2] Starting to build database from " + infiles + " ...");
            //FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
            long initTime = System.nanoTime();
            long endTime;


            //SequenceReader my_reader = new FastaSequenceReader(sequ2taxid, infoMode);

            if(this.numPartitions == 1) {

                this.inputSequences = this.jsc.wholeTextFiles(infiles)
                        .flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
                        .persist(StorageLevel.MEMORY_AND_DISK_SER());

            }
            else {
                LOG.warn("Using " +this.numPartitions + " partitions ...");
                this.inputSequences = this.jsc.wholeTextFiles(infiles, this.numPartitions)
                        .flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
                        .repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_AND_DISK_SER());
            }


            LOG.info("Adding " + this.inputSequences.count() + " sequences.");
            //inputData = inputData.coalesce(this.numPartitions);

            //List<SequenceHeaderFilename> data = this.inputSequences.map(new Sequence2HeaderFilename()).collect();

            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty());

            this.targets_= this.targetPropertiesJavaRDD.collect();

            int i = 0;

            long j = -1;

            for (TargetProperty target: this.targets_) {
                //LOG.info("Target taxa is: " + target.getTax());

                if (target.getTax() == 0) { // Target is unranked. Rank it!
                    target.setTax(j);

                    long parent_id = this.find_taxon_id(target.getIdentifier()).getTaxonId();

                    this.taxa_.getTaxa_().put(j, new Taxon(j, parent_id, target.getIdentifier(), Taxonomy.Rank.Sequence));
                    --j;
                }

                this.name2tax_.put(target.getIdentifier(), i);
                i++;
            }

/*
			for (TargetProperty target: this.targets_) {

				this.name2tax_.put(target.getIdentifier(),(int) target.getTax());
				this.sid2gid_.put(target.getIdentifier(), i);
				i++;
			}
*/

            LOG.info("Size of name2tax_ is: " + this.name2tax_.size());

            this.nextTargetId_ = this.name2tax_.size();


            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");
                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);
                //.mapPartitionsWithIndex(new Sketcher2HashMap(this.name2tax_), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");
                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMultiMapGuava(), true);
                //.mapPartitionsWithIndex(new Sketcher2HashMultiMap(this.name2tax_), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");

                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .repartition(this.params.getPartitions())
                        .mapPartitions(new Sketcher2PairPartitions(this.name2tax_), true)
                        .mapPartitions(new Locations2HashMapNative(), true);

                /*
                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true);
                        */

                /*
                this.locationJavaRDDHashMultiMapNative = this.inputSequences.zipWithIndex()
                        .mapToPair(t1 -> new Tuple2<>(t1._2, t1._1) )
                        .partitionBy(new MyCustomPartitionerLong(this.params.getPartitions()))
                        .values()
                        //.mapPartitionsWithIndex(new Sketcher2HashMultiMapNative(this.name2tax_), true);
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        //.partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        //.repartition(this.params.getPartitions())
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true);
                 */

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");
                this.locationJavaRDD = this.inputSequences
                        .flatMap(new Sketcher(this.name2tax_));

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");
				/*this.locationJavaPairListRDD = this.inputSequences
						.flatMapToPair(new Sketcher2Pair(this.name2tax_))
						.partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
						.combineByKey(new LocationCombiner(),
								new LocationMergeValues(),
								new LocationMergeCombiners());
								*/
                this.locationJavaPairIterableRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_)).groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }



        } catch (Exception e) {
            LOG.error("ERROR! "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    public void buildDatabaseMultiPartitions(ArrayList<String> infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {

            //FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
            long initTime = System.nanoTime();
            long endTime;

            JavaPairRDD<String,String> inputData = null;

            this.inputSequences = null;
            //JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;


            if(this.params.isMyWholeTextFiles()) {
				/*
			    if(this.numPartitions != 1) {
                    inputData = this.jsc.parallelize(infiles, this.numPartitions)
                            .mapPartitionsToPair(new MyWholeTextFiles(), true);
                }
                else {
                    inputData = this.jsc.parallelize(infiles)
                            .mapPartitionsToPair(new MyWholeTextFiles() );
                }
                */
                JavaRDD<String> tmpInput = this.jsc.parallelize(infiles);

                /*for (String currentDir : infiles) {
                    LOG.warn("Starting to build database from " + currentDir + " with MyWholeTextFiles");

                    if (tmpInput == null) {
                        tmpInput = this.jsc.wholeTextFiles(currentDir).keys();
                    } else {
                        tmpInput = this.jsc.wholeTextFiles(currentDir).keys()
                                .union(tmpInput);
                    }
                }

                int repartition_files = 1;

                if (this.numPartitions == 1) {
                    repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);
                }
                else {
                    repartition_files = this.numPartitions;
                }

                inputData = tmpInput
                        //.keys()
                        .repartition(repartition_files)
                        .mapPartitionsToPair(new MyWholeTextFiles(), true)
                        .persist(StorageLevel.DISK_ONLY());
*/


                //LOG.warn("Number of partitions for input files:" + inputData.count());

                int repartition_files = 1;

                if (this.numPartitions == 1) {
                    repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);

                }
                else {
                    repartition_files = this.numPartitions;
                }


                this.inputSequences = tmpInput
                        .mapPartitions(new Fasta2Sequence(sequ2taxid));

                //List<Long> lenghts_array = new ArrayList<>();

                List<Tuple2<Integer, String>> lenghts_array = this.inputSequences.mapToPair(item -> new Tuple2<Integer, String>(
                            item.getData().length(), item.getHeader()))
                        .sortByKey(false)
                        .collect();



                HashMap<String, Integer> all_lengths = new HashMap<>();

                int i = 0;
                for (Tuple2<Integer, String> item: lenghts_array) {

                    all_lengths.put(item._2, i);
                    ++i;
                    if (i >= this.numPartitions) {
                        i = 0;
                    }


                }


                this.inputSequences = tmpInput
                        .mapPartitions(new Fasta2Sequence(sequ2taxid))
                        //.repartition(repartition_files)
                        .mapToPair(item -> new Tuple2<String, Sequence>(item.getHeader(), item))
                        .partitionBy(new MyCustomPartitionerStr(this.numPartitions, all_lengths))
                        .values()
                        .persist(StorageLevel.MEMORY_AND_DISK());

                //.flatMap(new Fasta2Sequence(sequ2taxid))
                //.persist(StorageLevel.DISK_ONLY());


            }
            else {

                for (String currentDir : infiles) {
                    LOG.warn("Starting to build database from " + currentDir + " ...");

                    if (inputData == null) {
                        inputData = this.jsc.wholeTextFiles(currentDir);
                    } else {
                        inputData = this.jsc.wholeTextFiles(currentDir)
                                .union(inputData);
                    }
                }

                if(this.numPartitions != 1) {
                    LOG.info("Repartitioning into " + this.numPartitions + " partitions.");
                    this.inputSequences = inputData.flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
                            .repartition(this.numPartitions)
                            //.persist(StorageLevel.MEMORY_AND_DISK_SER());
                            .persist(StorageLevel.DISK_ONLY());
                }
                else {
                    LOG.info("No repartitioning");
                    this.inputSequences = inputData.flatMap(new FastaSequenceReader(sequ2taxid, infoMode));
                            //.persist(StorageLevel.MEMORY_AND_DISK_SER());
                            //.persist(StorageLevel.DISK_ONLY());
                }


            }
            //inputData = inputData.coalesce(this.numPartitions);

            //List<SequenceHeaderFilename> data = this.inputSequences.map(new Sequence2HeaderFilename()).collect();

            LOG.info("The number of sequences is: " + this.inputSequences.count());


            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty());

            this.targets_= this.targetPropertiesJavaRDD.collect();

            int i = 0;
            long j = -1;
            for (TargetProperty target: this.targets_) {
                if (target.getTax() == 0) { // Target is unranked. Rank it!
                    target.setTax(j);
                    this.taxa_.getTaxa_().put(j, new Taxon(j, 0, target.getIdentifier(), Taxonomy.Rank.Sequence));
                    --j;
                }
                this.name2tax_.put(target.getIdentifier(), i);
                i++;
            }

/*
			for (TargetProperty target: this.targets_) {

				this.name2tax_.put(target.getIdentifier(),(int) target.getTax());
				this.sid2gid_.put(target.getIdentifier(), i);
				i++;
			}
*/

            LOG.info("Size of name2tax_ is: " + this.name2tax_.size());

            this.nextTargetId_ = this.name2tax_.size();


            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");
                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);
                //.mapPartitionsWithIndex(new Sketcher2HashMap(this.name2tax_), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");
                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMultiMapGuava(), true);
                //.mapPartitionsWithIndex(new Sketcher2HashMultiMap(this.name2tax_), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");
                //this.locationJavaPairListRDD = this.inputSequences
                //		.mapPartitionsToPair(new Sketcher2PairPartitions(this.name2tax_), true);
                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        //.mapPartitionsWithIndex(new Sketcher2HashMultiMapNative(this.name2tax_), true);
                        //.repartition(this.params.getPartitions())
                        .mapPartitions(new Sketcher2PairPartitions(this.name2tax_), true)
                        .mapPartitions(new Locations2HashMapNative(), true);
                        //.mapPartitions(new PostProcessing(), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");
                this.locationJavaRDD = this.inputSequences
                        .flatMap(new Sketcher(this.name2tax_));

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");
				/*this.locationJavaPairListRDD = this.inputSequences
						.flatMapToPair(new Sketcher2Pair(this.name2tax_))
						.partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
						.combineByKey(new LocationCombiner(),
								new LocationMergeValues(),
								new LocationMergeCombiners());
								*/
                this.locationJavaPairIterableRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_)).groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }




            //endTime = System.nanoTime();
            //LOG.warn("Time in create database: "+ ((endTime - initTime)/1e9));
            //LOG.warn("Database created ...");
            //LOG.warn("Number of items into database: " + String.valueOf(this.locationJavaHashRDD.count()));
            //LOG.warn("Number of items into database: " + String.valueOf(this.locationJavaPairListRDD.count()));



        } catch (Exception e) {
            LOG.error("ERROR! "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void buildDatabaseInputFormat(ArrayList<String> infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {

            LOG.info("[JMAbuin] buildDatabaseInputFormat");
            //FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
            long initTime = System.nanoTime();
            long endTime;

            JavaPairRDD<String,String> inputData = null;

            this.inputSequences = null;
            //JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;


            JavaPairRDD<String, Text> reads = null;

            for (String currentFile : infiles) {

                if (reads == null) {
                    reads = this.jsc.newAPIHadoopFile(currentFile, FastaInputFormat.class, String.class, Text.class, this.jsc.hadoopConfiguration());
                }
                else {
                    reads = reads.union(this.jsc.newAPIHadoopFile(currentFile, FastaInputFormat.class, String.class, Text.class, this.jsc.hadoopConfiguration()));
                }

            }

            LOG.info("Reads from FastaInputFormat: " + reads.count());

            if(this.numPartitions != 1) {
                LOG.info("Repartitioning into " + this.numPartitions + " partitions.");
                this.inputSequences = reads.flatMap(new FastaSequenceReader2(sequ2taxid, infoMode))
                        .repartition(this.numPartitions)
                        //.persist(StorageLevel.MEMORY_AND_DISK_SER());
                        .persist(StorageLevel.DISK_ONLY());
            }
            else {
                LOG.info("No repartitioning");
                this.inputSequences = reads.flatMap(new FastaSequenceReader2(sequ2taxid, infoMode))
                        //.persist(StorageLevel.MEMORY_AND_DISK_SER());
                        .persist(StorageLevel.DISK_ONLY());
            }


            LOG.info("Adding " + this.inputSequences.count() + " sequences.");
            //inputData = inputData.coalesce(this.numPartitions);

            //List<SequenceHeaderFilename> data = this.inputSequences.map(new Sequence2HeaderFilename()).collect();

            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty());

            this.targets_= this.targetPropertiesJavaRDD.collect();

            int i = 0;

            long j = -1;

            for (TargetProperty target: this.targets_) {
                //LOG.info("Target taxa is: " + target.getTax());

                if (target.getTax() == 0) { // Target is unranked. Rank it!
                    target.setTax(j);

                    long parent_id = this.find_taxon_id(target.getIdentifier()).getTaxonId();

                    this.taxa_.getTaxa_().put(j, new Taxon(j, parent_id, target.getIdentifier(), Taxonomy.Rank.Sequence));
                    --j;
                }

                this.name2tax_.put(target.getIdentifier(), i);
                i++;
            }

/*
			for (TargetProperty target: this.targets_) {

				this.name2tax_.put(target.getIdentifier(),(int) target.getTax());
				this.sid2gid_.put(target.getIdentifier(), i);
				i++;
			}
*/

            LOG.info("Size of name2tax_ is: " + this.name2tax_.size());

            this.nextTargetId_ = this.name2tax_.size();


            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");
                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);
                //.mapPartitionsWithIndex(new Sketcher2HashMap(this.name2tax_), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");
                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMultiMapGuava(), true);
                //.mapPartitionsWithIndex(new Sketcher2HashMultiMap(this.name2tax_), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");

                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .repartition(this.params.getPartitions())
                        .mapPartitions(new Sketcher2PairPartitions(this.name2tax_), true)
                        .mapPartitions(new Locations2HashMapNative(), true);

                /*
                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true);
                        */

                /*
                this.locationJavaRDDHashMultiMapNative = this.inputSequences.zipWithIndex()
                        .mapToPair(t1 -> new Tuple2<>(t1._2, t1._1) )
                        .partitionBy(new MyCustomPartitionerLong(this.params.getPartitions()))
                        .values()
                        //.mapPartitionsWithIndex(new Sketcher2HashMultiMapNative(this.name2tax_), true);
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        //.partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        //.repartition(this.params.getPartitions())
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true);
                 */

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");
                this.locationJavaRDD = this.inputSequences
                        .flatMap(new Sketcher(this.name2tax_));

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");
				/*this.locationJavaPairListRDD = this.inputSequences
						.flatMapToPair(new Sketcher2Pair(this.name2tax_))
						.partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
						.combineByKey(new LocationCombiner(),
								new LocationMergeValues(),
								new LocationMergeCombiners());
								*/
                this.locationJavaPairIterableRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_)).groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }




            //endTime = System.nanoTime();
            //LOG.warn("Time in create database: "+ ((endTime - initTime)/1e9));
            //LOG.warn("Database created ...");
            //LOG.warn("Number of items into database: " + String.valueOf(this.locationJavaHashRDD.count()));
            //LOG.warn("Number of items into database: " + String.valueOf(this.locationJavaPairListRDD.count()));



        } catch (Exception e) {
            LOG.error("ERROR! "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    public void writeTaxonomy() {

        String taxonomyDestination = this.dbfile+"_taxonomy";

        this.taxa_.write(taxonomyDestination, this.jsc);

    }

    public void readTaxonomy() {

        String taxonomyDestination = this.dbfile+"_taxonomy";

        this.taxa_ = new Taxonomy();

        this.taxa_.read(taxonomyDestination, this.jsc);

    }

    public void writeTargets() {

        String targetsDestination = this.dbfile+"_targets";

        //this.targetPropertiesJavaRDD.saveAsObjectFile(targetsDestination);
        //this.targets_ = new ArrayList<TargetProperty>(inputSequences.map(new Sequence2TargetProperty()).collect());

        try {


            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(targetsDestination));

            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(this.targets_);

            oos.close();
            outputStream.close();
			/*
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
			FSDataOutputStream outputStream = fs.create(new Path(targetsDestination));

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

			for(int i = 0; i< this.targets_.size(); i++) {
				bw.write(i+":"+this.targets_.get(i).getIdentifier());
				bw.newLine();
			}

			bw.close();
			outputStream.close();
*/

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ targetsDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ targetsDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }



    }

    public void readTargets() {

        String targetsDestination = this.dbfile+"_targets";

        //this.targetPropertiesJavaRDD = this.jsc.objectFile(this.dbfile+"_targets");

        //this.targets_ = new ArrayList<TargetProperty>(this.targetPropertiesJavaRDD.collect());


        try {


            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(targetsDestination));

            ObjectInputStream ois = new ObjectInputStream(inputStream);
            this.targets_ = (List<TargetProperty>) ois.readObject();

            ois.close();
            inputStream.close();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ targetsDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ targetsDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

    }

    public void writeName2tax() {

        String name2taxDestination = this.dbfile+"_name2tax";

        //this.targetPropertiesJavaRDD.saveAsObjectFile(targetsDestination);
        //this.targets_ = new ArrayList<TargetProperty>(inputSequences.map(new Sequence2TargetProperty()).collect());

        try {


            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(name2taxDestination));

            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(this.name2tax_);

            oos.close();
            outputStream.close();
/*
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
			FSDataOutputStream outputStream = fs.create(new Path(name2taxDestination));

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

			Set<Map.Entry<String, Integer>> entries = this.name2tax_.entrySet();
			int i = 0;

			for(Map.Entry<String, Integer> entry:entries) {
				bw.write(i+":"+entry.getKey()+":"+entry.getValue());
				bw.newLine();
				++i;
			}

			bw.close();
			outputStream.close();*/
        }
        catch (IOException e) {
            LOG.error("Could not write file "+ name2taxDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ name2taxDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

    }

    public void readName2tax() {

        String name2taxDestination = this.dbfile+"_name2tax";

        //this.targetPropertiesJavaRDD = this.jsc.objectFile(this.dbfile+"_targets");

        //this.targets_ = new ArrayList<TargetProperty>(this.targetPropertiesJavaRDD.collect());


        try {


            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(name2taxDestination));

            ObjectInputStream ois = new ObjectInputStream(inputStream);
            this.name2tax_ = (TreeMap<String, Integer>) ois.readObject();

            ois.close();
            inputStream.close();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ name2taxDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ name2taxDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

    }



    public void writeSid2gid() {

        String sig2sidDestination = this.dbfile+"_sig2sid";

        // Try to open the filesystem (HDFS) and sequence file
        try {


            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(sig2sidDestination));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

            // Write data
            bw.write(String.valueOf(this.name2tax_.size()));
            bw.newLine();

            StringBuffer currentLine = new StringBuffer();

            for(Map.Entry<String, Integer> currentEntry: this.name2tax_.entrySet()) {

                currentLine.append(currentEntry.getKey());
                currentLine.append(":");
                currentLine.append(currentEntry.getValue());
                bw.write(currentLine.toString());
                bw.newLine();

                currentLine.delete(0, currentLine.toString().length());

            }

            bw.close();
            outputStream.close();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ sig2sidDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ sig2sidDestination+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

    }

    public void readSid2gid() {

        String sig2sidDestination = this.dbfile+"_sig2sid";

        try {
            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(sig2sidDestination));

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

            if(this.name2tax_ == null) {
                this.name2tax_ = new TreeMap<String, Integer>();
            }

            // read data
            long numberItems = Long.parseLong(br.readLine());

            String currentLine;

            while((currentLine = br.readLine()) != null) {

                String parts[] = currentLine.split(":");

                this.name2tax_.put(parts[0], Integer.parseInt(parts[1]));

            }


            br.close();
            inputStream.close();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ sig2sidDestination+ " because of IO error in readSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ sig2sidDestination+ " because of IO error in readSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }


    }

    // These infilenames are where assembly_summary.txt found are
    public HashMap<String, Long> make_sequence_to_taxon_id_map(ArrayList<String> mappingFilenames,ArrayList<String> infilenames) {
        //HashMap<String, Long> make_sequence_to_taxon_id_map(ArrayList<String> mappingFilenames,String infilenames)	{
        //gather all taxonomic mapping files that can be found in any
        //of the input directories

        HashMap<String, Long> map = new HashMap<String, Long>();

        //String dir = infilenames.get(0);

        //for(String newFile: mappingFilenames) {
        for(String newFile: infilenames) {
            //System.err.println("[JMAbuin] Accessing file: " + newFile + " in make_sequence_to_taxon_id_map");
            read_sequence_to_taxon_id_mapping(newFile, map);
        }

        return map;

    }

    public void read_sequence_to_taxon_id_mapping(String mappingFile, HashMap<String, Long> map){


        try {
            //JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());
            FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(mappingFile));

            BufferedReader d = new BufferedReader(new InputStreamReader(inputStream));

            //read first line(s) and determine the columns which hold
            //sequence ids (keys) and taxon ids
            int headerRow = 0;
            String newLine = d.readLine();

            while(newLine != null) {

                if(newLine.startsWith("#")) {
                    headerRow++;
                    newLine = d.readLine();
                }
                else {
                    break;
                }

            }

            headerRow--;

            d.close();
            inputStream.close();

            //reopen and forward to header row
            inputStream = fs.open(new Path(mappingFile));
            d = new BufferedReader(new InputStreamReader(inputStream));

            for(int i = 0; i< headerRow; i++) {
                newLine = d.readLine();
            }

            //process header row
            int keycol = 0;
            int taxcol = 0;
            int col = 0;
            String header = d.readLine();

            header = header.replaceFirst("#", "");

            String headerSplits[] = header.split("\t");

            for(String headerField: headerSplits) {
                //System.err.println("[JMAbuin] header split " + headerField.trim());
                if(headerField.trim().equals("taxid")) {
                    taxcol = col;
                }
                else if (header.trim().equals("accession.version") || header.trim().equals("assembly_accession")) {
                    keycol = col;
                }
                col++;

            }

            //taxid column assignment not found
            //use 1st column as key and 2nd column as taxid
            if(taxcol == 0 && keycol == 0) { //keycol is already 0
                taxcol = 1;
            }

            String key;
            Long taxonId;

            newLine = d.readLine();

            while(newLine != null) {

                String lineSplits[] = newLine.split("\t");

                key = lineSplits[keycol];
                taxonId = Long.parseLong(lineSplits[taxcol]);

                map.put(key, taxonId);

                newLine = d.readLine();

            }
            //System.err.println("[JMAbuin] End of read_sequence_to_taxon_id_mapping");
            d.close();
            inputStream.close();
            //fs.close();

        }
        catch (IOException e) {
            e.printStackTrace();
            LOG.error("I/O Error accessing HDFS in read_sequence_to_taxon_id_mapping: "+e.getMessage());
            System.exit(1);
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("General error accessing HDFS in read_sequence_to_taxon_id_mapping: "+e.getMessage());
            System.exit(1);
        }


        //return map;


    }


    public ArrayList<Long> unranked_targets() {
        ArrayList<Long> res = new ArrayList<Long>();

        for(long i = 0; i < this.target_count(); ++i) {
            if(this.taxon_of_target(i) == null) {
                res.add(i);
            }
        }

        return res;
    }

    public Integer target_id_of_sequence(String sid) {
        //String it = name2tax_..find(sid);

        if(this.name2tax_.containsKey(sid)) {
            return this.name2tax_.get(sid);
        }
        else {
            return (int)nextTargetId_;
        }

        //return (it != name2tax_.end()) ? it->second : nextTargetId_;
    }

    public boolean is_valid(int tid) {
        return tid < nextTargetId_;
    }

    public void rank_target(int tid, long taxid) {
        targets_.get(tid).setTax(taxid);
        update_lineages(targets_.get(tid));
    }

    public Long[] ranks_of_target(int id)  {
        return targets_.get(id).getRanked_lineage();
    }

    //public void try_to_rank_unranked_targets(database& db, const build_param& param)
    public void try_to_rank_unranked_targets() {
        ArrayList<Long> unranked = this.unranked_targets();

        if(!unranked.isEmpty()) {
            LOG.info(unranked.size() + " targets could not be ranked.");

            for(String file : this.taxonomyParam.getMappingPostFiles()) {
                this.rank_targets_post_process(unranked, file);
            }
        }

        unranked = this.unranked_targets();

        if(unranked.isEmpty()) {
            LOG.info("All targets are ranked.");
        }
        else {
            LOG.info(unranked.size() + " targets remain unranked.");
        }

    }

    public void rank_targets_post_process(ArrayList<Long> gids, String mappingFile)	{

        if(gids.isEmpty()) return;

        try {
            LOG.info("Try to map sequences to taxa using '" + mappingFile);

            //JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());
            FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

            if(!fs.isFile(new Path(mappingFile))) {
                return;
            }

            FSDataInputStream inputStream = fs.open(new Path(mappingFile));

            BufferedReader d = new BufferedReader(new InputStreamReader(inputStream));

            String acc;
            String accver;
            Long taxid;
            String gi;


            String newLine = d.readLine();

            while(newLine != null) {

                //skip header
                if(newLine.startsWith("#")) {
                    newLine = d.readLine();
                }
                else {

                    String lineSplits[] = newLine.split("\t");

                    int i = 0;

                    while(i+4<lineSplits.length) {

                        acc = lineSplits[i];
                        accver = lineSplits[i+1];
                        taxid = Long.valueOf(lineSplits[i+2]);
                        gi = lineSplits[i+3];

                        //target in database?
                        //accession.version is the default
                        int tid = this.target_id_of_sequence(accver);

                        if(!this.is_valid(tid)) {
                            tid = this.target_id_of_sequence(acc);
                            if(!this.is_valid(tid)) {
                                tid = this.target_id_of_sequence(gi);
                            }
                        }

                        //if in database then map to taxon
                        if(this.is_valid(tid)) {

                            int pos = 0;
                            for(Long current: gids){
                                if(current == tid) {
                                    break;
                                }
                                pos++;
                            }

                            if(pos != gids.size()){


                                this.rank_target(tid, taxid);
                                gids.remove(pos);

                                if(gids.isEmpty()) {
                                    break;
                                }
                            }

                        }

                        i+=4;
                    }

                }

            }

            d.close();
            inputStream.close();
            //fs.close();
        }

        catch (IOException e) {
            LOG.error("I/O Error accessing HDFS in rank_targets_post_process: "+e.getMessage());
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("General error accessing HDFS in rank_targets_post_process: "+e.getMessage());
            //System.exit(1);
        }

    }


    public void remove_ambiguous_features(Taxonomy.Rank r, int maxambig) {
        if(this.taxa_.empty()) {
            LOG.error("No taxonomy available!");
            System.exit(1);
        }

        if(maxambig == 0) maxambig = 1;
        //Todo: Do it with Spark
/*
		if(r == Taxonomy.Rank.Sequence) {
			long i = 0;
			long e = features_.count();

			for(; i != e; ++i) {
				Row currentFeature = features_.coll

				if(!i->empty()) {
					std::set<target_id> targets;
					for(auto loc : *i) {
						targets.insert(loc.tgt);
						if(targets.size() > maxambig) {
							features_.clear(i);
							break;
						}
					}
				}
			}
		}
		else {
			for(auto i = features_.begin(), e = features_.end(); i != e; ++i) {
				if(!i->empty()) {
					std::set<taxon_id> taxa;
					for(auto loc : *i) {
						taxa.insert(targets_[loc.tgt].ranks[int(r)]);
						if(taxa.size() > maxambig) {
							features_.clear(i);
							break;
						}
					}
				}
			}
		}*/
    }

    public void write_database() {


        //this.locationJavaPairRDD.saveAsObjectFile(this.dbfile);

        try {

            //JavaSparkContext javaSparkContext = new JavaSparkContext(sparkS.sparkContext());
            //FileSystem fs = FileSystem.get(javaSparkContext.hadoopConfiguration());
            FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

            String path = fs.getHomeDirectory().toString();

            long startTime = System.nanoTime();

            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                //this.locationJavaHashRDD.saveAsObjectFile(path+"/"+this.dbfile);

                List<String> outputs = this.locationJavaHashRDD
                        .mapPartitionsWithIndex(new WriteHashMap(path+"/"+this.dbfile), true)
                        .collect();

                for(String output: outputs) {
                    LOG.warn("Wrote file: "+output);
                }

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) {//(paramsBuild.isBuildModeHashMultiMapG()) {
                //this.locationJavaHashMMRDD.saveAsObjectFile(path+"/"+this.dbfile);
                List<String> outputs = this.locationJavaHashMMRDD
                        .mapPartitionsWithIndex(new WriteHashMultiMapGuava(path+"/"+this.dbfile), true)
                        .collect();

                for(String output: outputs) {
                    LOG.warn("Wrote file: "+output);
                }
            }
            else if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {
                //this.locationJavaPairListRDD.saveAsObjectFile(path+"/"+this.dbfile);
                //this.locationJavaRDDHashMultiMapNative.saveAsObjectFile(path+"/"+this.dbfile);

                List<String> outputs = this.locationJavaRDDHashMultiMapNative.mapPartitionsWithIndex(
                        new WriteHashMapNative(path+"/"+this.dbfile), true).collect();

                for(String output: outputs) {
                    LOG.warn("Writed file: "+output);
                }

            }
            else if(this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {
                this.featuresDataframe_.write().parquet(path+"/"+this.dbfile);
                //this.featuresDataframe_.write().partitionBy("key").parquet(path+"/"+this.dbfile);
            }
            else if(this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) {
                //this.locationJavaPairListRDD.saveAsObjectFile(path+"/"+this.dbfile);

                //this.locationsRDD.saveAsObjectFile(path+"/"+this.dbfile);
                List<String> outputs = this.locationsRDD.mapPartitionsWithIndex(new WriteLocations(path+"/"+this.dbfile), true)
                        .collect();

                for(String file_name: outputs) {
                    LOG.info("File written: " + file_name);
                }

            }



            //LOG.warn("Time in writedatabase is: "+ (System.nanoTime() - startTime)/10e9);


            //this.locationJavaPairListRDD.saveAsObjectFile(this.dbfile);
            //this.locationJavaHashRDD.saveAsObjectFile(this.dbfile);

            //this.featuresDataframe_.unpersist();

            LOG.info("Database created at "+ path+"/"+this.dbfile);

        }
        catch (IOException e) {
            LOG.error("I/O Error accessing HDFS in write_database: "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        catch (Exception e) {
            LOG.error("General error accessing HDFS in write_database: "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

    }

    public void loadFromFile() {


        /*
         * Ordering<Integer> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<Integer>naturalOrder());
         */
        if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {
            LOG.warn("Loading database in Java HashMap format");
            //this.locationJavaPairListRDD = JavaPairRDD.fromJavaRDD(this.jsc.objectFile(this.dbfile));

            List<String> filesNames = FilesysUtility.files_in_directory(this.dbfile, 0, this.jsc);

            for(String newFile : filesNames) {
                LOG.warn("New file added: "+newFile);
            }

            JavaRDD<String> filesNamesRDD;

            if(this.numPartitions != 1) {
                filesNamesRDD = this.jsc.parallelize(filesNames, this.numPartitions);
            }
            else {
                filesNamesRDD = this.jsc.parallelize(filesNames);
            }


            this.locationJavaHashRDD = filesNamesRDD
                    .mapPartitionsWithIndex(new ReadHashMap(), true)
                    .persist(StorageLevel.MEMORY_AND_DISK());

			/*

            this.locationJavaHashRDD = this.jsc.objectFile(this.dbfile);

            this.locationJavaHashRDD.persist(StorageLevel.MEMORY_AND_DISK());
*/
            LOG.warn("The number of paired persisted entries is: " + this.locationJavaHashRDD.count());


        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) {
			/*LOG.warn("Loading database with isBuildModeHashMultiMapG");
			this.locationJavaHashMMRDD = this.jsc.objectFile(this.dbfile);

			this.locationJavaHashMMRDD.persist(StorageLevel.MEMORY_AND_DISK());

			LOG.warn("The number of persisted HashMultimaps is: " + this.locationJavaHashMMRDD.count());
			*/

            LOG.warn("Loading database in Guava HashMultimap format");



            List<String> filesNames = FilesysUtility.files_in_directory(this.dbfile, 0, this.jsc);

            for(String newFile : filesNames) {
                LOG.warn("New file added: "+newFile);
            }

            JavaRDD<String> filesNamesRDD;

            if(this.numPartitions != 1) {
                filesNamesRDD = this.jsc.parallelize(filesNames, this.numPartitions);
            }
            else {
                filesNamesRDD = this.jsc.parallelize(filesNames);
            }


            this.locationJavaHashMMRDD = filesNamesRDD
                    .mapPartitionsWithIndex(new ReadHashMapGuava(), true)
                    .persist(StorageLevel.MEMORY_AND_DISK());


/*
            this.locationJavaHashMMRDD = this.jsc.objectFile(this.dbfile);

            this.locationJavaHashMMRDD.persist(StorageLevel.MEMORY_AND_DISK());
*/
            LOG.warn("The number of paired persisted entries is: " + this.locationJavaHashMMRDD.count());

        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {
            LOG.warn("Loading database with isBuildModeHashMultiMapMC");
            //this.locationJavaPairListRDD = JavaPairRDD.fromJavaRDD(this.jsc.objectFile(this.dbfile));

            List<String> filesNames = FilesysUtility.files_in_directory(this.dbfile, 0, this.jsc);

            for(String newFile : filesNames) {
                LOG.warn("New file added: "+newFile);
            }

            JavaRDD<String> filesNamesRDD;

            if(this.numPartitions != 1) {
                filesNamesRDD = this.jsc.parallelize(filesNames, this.numPartitions);
            }
            else {
                filesNamesRDD = this.jsc.parallelize(filesNames);
            }


            this.locationJavaRDDHashMultiMapNative = filesNamesRDD
                    .mapPartitionsWithIndex(new ReadHashMapNative(), true)
                    .persist(StorageLevel.MEMORY_AND_DISK());


            LOG.warn("The number of paired persisted entries is: " + this.locationJavaRDDHashMultiMapNative.count());
        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {
            LOG.warn("Loading database with isBuildModeParquetDataframe");
            //DataFrame dataFrame = this.sqlContext.read().parquet(this.dbfile);
            Dataset<Location> dataFrame = this.sqlContext.read().parquet(this.dbfile).map(new Row2Location(), Encoders.bean(Location.class));

            this.locationJavaRDD = dataFrame.javaRDD();

            this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class))
                    .persist(StorageLevel.MEMORY_AND_DISK());

            this.featuresDataframe_.registerTempTable("MetaCacheSpark");
            //this.sqlContext.cacheTable("MetaCacheSpark");

            LOG.warn("The number of paired persisted entries is: " + this.featuresDataframe_.count());

        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) {
            LOG.warn("Loading database with isBuildCombineByKey");

            Dataset<Location> dataFrame = this.sqlContext.read().parquet(this.dbfile).map(new Row2Location(), Encoders.bean(Location.class));

            /*
            if(this.numPartitions != 1) {
                this.locationJavaRDD = dataFrame.javaRDD().repartition(this.numPartitions);
            }
            else {
                this.locationJavaRDD = dataFrame.javaRDD();
            }
            */
            this.locationJavaRDD = dataFrame.javaRDD();

            this.locationsRDD = this.locationJavaRDD.mapPartitionsToPair(new Location2Pair(), true)
                    .groupByKey()
                    .map(new LocationKeyIterable2Locations(this.params.getProperties().getMax_locations_per_feature()));

            if(this.numPartitions != 1) {
                this.featuresDataset = this.sqlContext.createDataset(this.locationsRDD.rdd(), Encoders.bean(Locations.class))
                        .repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_AND_DISK());
            }
            else {
                this.featuresDataset = this.sqlContext.createDataset(this.locationsRDD.rdd(), Encoders.bean(Locations.class))
                        .persist(StorageLevel.MEMORY_AND_DISK());
            }



            this.featuresDataset.registerTempTable("MetaCacheSpark");
            //this.sqlContext.cacheTable("MetaCacheSpark");

            LOG.warn("The number of paired persisted entries is: " + this.featuresDataset.count());


        }

    }

    public List<TreeMap<LocationBasic, Integer>> accumulate_matches_buffer_treemap2( List<SequenceData> inputData, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                .mapPartitionsToPair(new PartialQueryTreeMap(inputData, init, size, total, readed))
                .reduceByKey(new QueryReducerTreeMap())
                .sortByKey()
                .values()
                .collect();

        long endTime = System.nanoTime();

        LOG.warn("JMAbuin time in insert into treeMap is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }

    public List<TreeMap<LocationBasic, Integer>> accumulate_matches_buffer_treemap_local( String file_name, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                .mapPartitionsToPair(new PartialQueryTreeMapLocal(file_name, init, size, total, readed))
                .reduceByKey(new QueryReducerTreeMap())
                .sortByKey()
                .values()
                .collect();

        long endTime = System.nanoTime();

        LOG.warn("JMAbuin time in insert into treeMap is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }

    public List<TreeMap<LocationBasic, Integer>>  accumulate_matches_full_native_buffered( String fileName, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        //List<Tuple2<Long,HashMap<LocationBasic, Integer>>> results = this.locationJavaRDDHashMultiMapNative
        List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                //.mapPartitionsToPair(new FullQuery(fileName))
                .mapPartitionsToPair(new PartialQueryNative(fileName, init, size, total, readed, this.params.getResult_size()))
                .reduceByKey(new QueryReducerTreeMapNative())
                .sortByKey()
                .values()
                .collect();

        long endTime = System.nanoTime();

        LOG.warn("JMAbuin time in insert into HashMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }


    public List<TreeMap<LocationBasic, Integer>>  accumulate_matches_native_buffered( String fileName, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        if (this.params.getResult_size() > 0) {
            List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                    //.mapPartitionsToPair(new FullQuery(fileName))
                    .mapPartitionsToPair(new PartialQueryNativeTreeMapBest(fileName, init, size, total, readed, this.params.getResult_size()))
                    .reduceByKey(new QueryReducerTreeMapNative())
                    .sortByKey()
                    .values()
                    .collect();

            long endTime = System.nanoTime();

            LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

            return results;
        }
        else {
            List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                    //.mapPartitionsToPair(new FullQuery(fileName))
                    .mapPartitionsToPair(new PartialQueryNativeTreeMap(fileName, init, size, total, readed, this.params.getResult_size()))
                    .reduceByKey(new QueryReducerTreeMapNative())
                    .sortByKey()
                    .values()
                    .collect();

            long endTime = System.nanoTime();

            LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

            return results;
        }

    }

    public List<TreeMap<LocationBasic, Integer>>  accumulate_matches_native_buffered_paired( String fileName, String fileName2, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        if (this.params.getResult_size() > 0) {
            List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                    //.mapPartitionsToPair(new FullQuery(fileName))
                    .mapPartitionsToPair(new PartialQueryNativeTreeMapBestPaired(fileName, fileName2, init, size, total, readed, this.params.getResult_size()))
                    .reduceByKey(new QueryReducerTreeMapNative())
                    .sortByKey()
                    .values()
                    .collect();

            long endTime = System.nanoTime();

            LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

            return results;
        }
        else {
            List<TreeMap<LocationBasic, Integer>> results = this.locationJavaRDDHashMultiMapNative
                    //.mapPartitionsToPair(new FullQuery(fileName))
                    .mapPartitionsToPair(new PartialQueryNativeTreeMapPaired(fileName, fileName2, init, size, total, readed, this.params.getResult_size()))
                    .reduceByKey(new QueryReducerTreeMapNative())
                    .sortByKey()
                    .values()
                    .collect();

            long endTime = System.nanoTime();

            LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

            return results;
        }

    }

    public List<List<MatchCandidate>>  accumulate_matches_native_buffered_paired_list( String fileName, String fileName2,
                                                                                      long init, int size) {

        long initTime = System.nanoTime();

        List<List<MatchCandidate>> results = this.locationJavaRDDHashMultiMapNative
                //.mapPartitionsToPair(new FullQuery(fileName))
                .mapPartitionsToPair(new PartialQueryNativeListPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params,
                        this.taxa_, this.targets_))
                .reduceByKey(new QueryReducerListNative())
                .sortByKey()
                .values()
                .collect();

        long endTime = System.nanoTime();

        LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }

    //public TreeMap<LocationBasic, Integer> accumulate_matches_hashmapnativemode(String file_name, long query_number) {
    public TreeMap<LocationBasic, Integer> accumulate_matches_native_single(String file_name, long query_number) {

        //long initTime = System.nanoTime();

        // Get results for this buffer
        //List<Tuple2<Long,HashMap<LocationBasic, Integer>>> results = this.locationJavaRDDHashMultiMapNative
        TreeMap<LocationBasic, Integer> results = this.locationJavaRDDHashMultiMapNative
                .map(new PartialQueryNativeTreeMapSingle(file_name, query_number, this.params.getResult_size()))
                .reduce(new QueryReducerTreeMapNative());


        //long endTime = System.nanoTime();

        //LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;

    }

    public List<TreeMap<LocationBasic, Integer>>  accumulate_matches_hashmap_java_buffered( String fileName, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        //List<Tuple2<Long,HashMap<LocationBasic, Integer>>> results = this.locationJavaRDDHashMultiMapNative
        List<TreeMap<LocationBasic, Integer>> results = this.locationJavaHashRDD
                //.mapPartitionsToPair(new FullQuery(fileName))
                .mapPartitionsToPair(new PartialQueryJavaTreeMap(fileName, init, size, total, readed, this.params.getResult_size()))
                .reduceByKey(new QueryReducerTreeMapNative())
                .sortByKey()
                .values()
                .collect();

        long endTime = System.nanoTime();

        LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }

    public TreeMap<LocationBasic, Integer> accumulate_matches_hashmap_java_single(String file_name, long query_number) {

        //long initTime = System.nanoTime();

        // Get results for this buffer
        //List<Tuple2<Long,HashMap<LocationBasic, Integer>>> results = this.locationJavaRDDHashMultiMapNative
        TreeMap<LocationBasic, Integer> results = this.locationJavaHashRDD
                .map(new PartialQueryJavaTreeMapSingle(file_name, query_number, this.params.getResult_size()))
                .reduce(new QueryReducerTreeMapNative());


        //long endTime = System.nanoTime();

        //LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;

    }

    public List<TreeMap<LocationBasic, Integer>>  accumulate_matches_hashmap_guava_buffered( String fileName, long init, int size, long total, long readed) {

        long initTime = System.nanoTime();

        // Get results for this buffer
        //List<Tuple2<Long,HashMap<LocationBasic, Integer>>> results = this.locationJavaRDDHashMultiMapNative
        List<TreeMap<LocationBasic, Integer>> results = this.locationJavaHashMMRDD
                //.mapPartitionsToPair(new FullQuery(fileName))
                .mapPartitionsToPair(new PartialQueryGuavaTreeMap(fileName, init, size, total, readed, this.params.getResult_size()))
                .reduceByKey(new QueryReducerTreeMapNative())
                .sortByKey()
                .values()
                .collect();

        long endTime = System.nanoTime();

        LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }

    public TreeMap<LocationBasic, Integer> accumulate_matches_hashmap_guava_single(String file_name, long query_number) {

        //long initTime = System.nanoTime();

        // Get results for this buffer
        //List<Tuple2<Long,HashMap<LocationBasic, Integer>>> results = this.locationJavaRDDHashMultiMapNative
        TreeMap<LocationBasic, Integer> results = this.locationJavaHashMMRDD
                .map(new PartialQueryGuavaTreeMapSingle(file_name, query_number, this.params.getResult_size()))
                .reduce(new QueryReducerTreeMapNative());


        //long endTime = System.nanoTime();

        //LOG.warn("JMAbuin time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;

    }


    //public TreeMap<LocationBasic, Integer> matches(Sketch query) {
    public TreeMap<LocationBasic, Integer> matches(SequenceData query) {

        if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {
            return this.accumulate_matches_hashmapmode(query);
        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) {
            return null;
        }
        //else if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {
        //	return this.accumulate_matches_native_treemap_single(query);
        //}
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {
            return this.accumulate_matches_filter(query);
        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) {
            return this.accumulate_matches_combinemode(query);
            //return this.accumulate_matches_filter(query);
            //return this.accumulate_matches_combine(query);
            //return this.accumulate_matches_combine_RDD(query);
        }

        return null;

    }

    public TreeMap<LocationBasic, Integer> matches(String file_name, long query_number) {

		/*if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {
			return this.accumulate_matches_hashmapmode(query);
		}
		else if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) {
			return null;
		}
		else*/ if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {
            return this.accumulate_matches_native_single(file_name, query_number);
        }/*
		else if(this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {
			return this.accumulate_matches_filter(query);
		}
		else if(this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) {
			return this.accumulate_matches_combinemode(query);
			//return this.accumulate_matches_filter(query);
			//return this.accumulate_matches_combine(query);
			//return this.accumulate_matches_combine_RDD(query);
		}*/

        return null;

    }



    public List<TreeMap<LocationBasic, Integer>> matches_buffered(List<SequenceData> queries, long init, int size, long total, long readed) {

        if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {
            LOG.warn("[JMAbuin] Using buffered mode");
            return this.accumulate_matches_buffer_treemap2(queries, init, size, total, readed);
            //return this.accumulate_matches_buffer_treemap(queries, init, size, total, readed);
        }

        return null;
    }

    public List<TreeMap<LocationBasic, Integer>> matches_buffered_local(String file_name, long init, int size, long total, long readed) {

        if(this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {
            LOG.warn("[JMAbuin] Using buffered_local mode");
            return this.accumulate_matches_buffer_treemap_local(file_name, init, size, total, readed);
            //return this.accumulate_matches_buffer_treemap(queries, init, size, total, readed);
        }

        return null;
    }


    public TreeMap<LocationBasic, Integer> accumulate_matches_filter(SequenceData query) {

        //TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());
        TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>();

        StringBuilder queryString = new StringBuilder();

        JavaRDD<LocationBasic> obtainedLocationsRDD = null;

        List<LocationBasic> obtainedLocations = new ArrayList<LocationBasic>();

        try {

            ArrayList<Sketch> locations = SequenceFileReader.getSketchStatic(query);

            for(Sketch currentSketch : locations) {
                for (int i : currentSketch.getFeatures()) {
/*
				if(queryString.toString().isEmpty()) {
					queryString.append("select * from MetaCacheSpark where key = " + i);
				}
				else {
					queryString.append(" or key = "+i);
				}
*/
                    //Row results[] = this.featuresDataframe_.filter(this.featuresDataframe_.col("key").equalTo(i)).collect();
                    List<Location> results = this.featuresDataframe_.filter(this.featuresDataframe_.col("key").equalTo(i)).collectAsList();

                    for (Location current_location : results) {
                        //ArrayList<LocationBasic> myList = (ArrayList<LocationBasic>) IOSerialize.fromString(currentRow.getString(1));

                        obtainedLocations.add(new LocationBasic(current_location.getTargetId(), current_location.getWindowId()));


                    }

                }
            }


            //obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Row2Location());

            //obtainedLocations = obtainedLocationsRDD.collect();

            //queryString.delete(0, queryString.toString().length());

            for (LocationBasic obtainedLocation : obtainedLocations) {

                if (res.containsKey(obtainedLocation)) {
                    res.put(obtainedLocation, res.get(obtainedLocation) + 1);
                } else {
                    res.put(obtainedLocation, 1);
                }

            }


            return res;

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        return res;
        //}

    }


    public TreeMap<LocationBasic, Integer> accumulate_matches_combinemode(SequenceData query) {

        //TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());
        TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>();

        try {

            ArrayList<Sketch> locations = SequenceFileReader.getSketchStatic(query);

            for(Sketch currentSketch : locations) {
                //for (int i : currentSketch.getFeatures()) {

                int[] current_features = currentSketch.getFeatures();

                List<Locations> obtainedValues = new ArrayList<Locations>();

                for(int current_feature: current_features) {
                    obtainedValues.addAll(this.featuresDataset.filter( "key=" + current_feature).collectAsList());
                }

                if(obtainedValues.size() > 1) {
                    LOG.warn("The obtained values from query is bigger than one!!!");
                }
                else if(obtainedValues.size() == 0) {
                    LOG.warn("The obtained values from query zero!!!");
                }

                for(Locations current_locations: obtainedValues) {
                    for (LocationBasic newLocation : current_locations.getLocations()) {

                        if (res.containsKey(newLocation)) {
                            res.put(newLocation, res.get(newLocation) + 1);
                        } else {
                            res.put(newLocation, 1);
                        }
                    }

                }

                //LOG.warn("[JMAbuin] Number of locations: "+res.size());
                //}
            }

            return res;
            //}

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        return res;

    }


    // TODO: Here!!!
    public TreeMap<LocationBasic, Integer> accumulate_matches_hashmapmode(SequenceData query) {

        //TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());
        TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>();

        try {
            ArrayList<Sketch> locations = SequenceFileReader.getSketchStatic(query);

            for(Sketch currentSketch : locations) {
                for (int i : currentSketch.getFeatures()) {

                    List<LocationBasic> obtainedValues = this.locationJavaHashRDD
                            .mapPartitions(new SearchInHashMap(i), true)
                            .collect();

                    //for(List<LocationBasic> obtainedLocations: obtainedValues){
                    for (LocationBasic obtainedLocation : obtainedValues) {

                        //Location newLocation = new Location(i, obtainedLocation.getTargetId(), obtainedLocation.getWindowId());

                        if (res.containsKey(obtainedLocation)) {
                            res.put(obtainedLocation, res.get(obtainedLocation) + 1);
                        } else {
                            res.put(obtainedLocation, 1);
                        }
                    }

                    //}

                }


                LOG.warn("[JMAbuin] Number of locations: " + res.size());
            }
            return res;
            //}

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        return res;

    }




    public TreeMap<Location, Integer> accumulate_matches(Sketch query) {

        TreeMap<Location, Integer> res = new TreeMap<Location, Integer>(new LocationComparator());

        StringBuilder queryString = new StringBuilder();

        JavaRDD<Location> obtainedLocationsRDD = null;

        List<Location> obtainedLocations = new ArrayList<Location>();

        try {


            for (int i : query.getFeatures()) {

                if(queryString.toString().isEmpty()) {
                    queryString.append("select * from MetaCacheSpark where key = " + i);
                }
                else {
                    queryString.append(" or key = "+i);
                }

            }

            obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Function<Row, Location>() {
                @Override
                public Location call(Row row) throws Exception {
                    return new Location(row.getInt(0), row.getInt(1), row.getInt(2));
                }
            });

            obtainedLocations = obtainedLocationsRDD.collect();

            queryString.delete(0, queryString.toString().length());

            for (Location obtainedLocation : obtainedLocations) {

                if (res.containsKey(obtainedLocation)) {
                    res.put(obtainedLocation, res.get(obtainedLocation) + 1);
                } else {
                    res.put(obtainedLocation, 1);
                }

            }


            return res;

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        return res;
        //}

    }

    public void accumulate_matches(Sketch query, TreeMap<LocationBasic, Integer> res) {
        try {
            List<LocationBasic> obtainedLocations = new ArrayList<LocationBasic>();

            JavaRDD<LocationBasic> obtainedLocationsRDD = null;
            // Made queries
            //for(Sketch currentLocation: query) {

            StringBuilder queryString = new StringBuilder();

            //for(Sketch currentSketch : query) {
            for (int i : query.getFeatures()) {
                //String queryString = "select * from parquetFeatures where key=" + i;
                //DataFrame result = this.sqlContext.sql(queryString);

                queryString.append("select * from MetaCacheSpark where key = " + i);

                obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Row2LocationBasic());

                obtainedLocations = obtainedLocationsRDD.collect();

                queryString.delete(0, queryString.toString().length());

                for (LocationBasic obtainedLocation : obtainedLocations) {

                    if (res.containsKey(obtainedLocation)) {
                        res.put(obtainedLocation, res.get(obtainedLocation) + 1);
                    } else {
                        res.put(obtainedLocation, 1);
                    }

                }
            }
            //}

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        //}
    }

    public TreeMap<Location, Integer> accumulate_matches(ArrayList<Sketch> query) {

        TreeMap<Location, Integer> res = new TreeMap<Location, Integer>(new LocationComparator());

        StringBuilder queryString = new StringBuilder();

        JavaRDD<Location> obtainedLocationsRDD = null;

        List<Location> obtainedLocations = new ArrayList<Location>();

        try {


            for(Sketch currentSketch : query) {
                for (int i : currentSketch.getFeatures()) {

                    //obtainedLocationsRDD = this.featuresDataframe_.filter("key ="+i).javaRDD().map(new Row2Location());


                    queryString.append("select * from MetaCacheSpark where key = " + i);

                    obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Function<Row, Location>() {
                        @Override
                        public Location call(Row row) throws Exception {
                            return new Location(row.getInt(0), row.getInt(1), row.getInt(2));
                        }
                    });

                    obtainedLocations = obtainedLocationsRDD.collect();

                    queryString.delete(0, queryString.toString().length());

                    for (Location obtainedLocation : obtainedLocations) {

                        if (res.containsKey(obtainedLocation)) {
                            res.put(obtainedLocation, res.get(obtainedLocation) + 1);
                        } else {
                            res.put(obtainedLocation, 1);
                        }

                    }
                }
            }

            return res;

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        return res;
        //}

    }

    public void accumulate_matches(ArrayList<Sketch> query, TreeMap<Location, Integer> res) {
        try {
            List<Location> obtainedLocations = new ArrayList<Location>();

            JavaRDD<Location> obtainedLocationsRDD = null;
            // Made queries
            //for(Sketch currentLocation: query) {

            StringBuilder queryString = new StringBuilder();

            for(Sketch currentSketch : query) {
                for (int i : currentSketch.getFeatures()) {
                    //String queryString = "select * from parquetFeatures where key=" + i;
                    //DataFrame result = this.sqlContext.sql(queryString);

                    if(queryString.toString().isEmpty()){
                        queryString.append("select * from MetaCacheSpark where key = " + i);
                    }
                    else {
                        queryString.append(" or key = " + i);
                    }


					/*obtainedLocationsRDD = this.featuresDataframe_.filter("key ="+i).javaRDD().map(new Row2Location());

					obtainedLocations = obtainedLocationsRDD.collect();

					//queryString.delete(0, queryString.toString().length());

					for (Location obtainedLocation : obtainedLocations) {

						if (res.containsKey(obtainedLocation)) {
							res.put(obtainedLocation, res.get(obtainedLocation) + 1);
						} else {
							res.put(obtainedLocation, 1);
						}

					}*/

                }

                obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Function<Row, Location>() {
                    @Override
                    public Location call(Row row) throws Exception {
                        return new Location(row.getInt(0), row.getInt(1), row.getInt(2));
                    }
                });

                obtainedLocations = obtainedLocationsRDD.collect();

                queryString.delete(0, queryString.toString().length());

                for (Location obtainedLocation : obtainedLocations) {

                    if (res.containsKey(obtainedLocation)) {
                        res.put(obtainedLocation, res.get(obtainedLocation) + 1);
                    } else {
                        res.put(obtainedLocation, 1);
                    }

                }

                LOG.warn("[JMAbuin] Number of locations: "+obtainedLocations.size());

            }

        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.error("General error in accumulate_matches: "+e.getMessage());
            System.exit(1);
        }

        //}
    }

    public static long invalid_target_id() {
        return max_target_count();
    }


    public static long max_target_count() {
        return Integer.MAX_VALUE;
    }

    public static long max_windows_per_target() {
        return Integer.MAX_VALUE;
    }

    public boolean empty() {
        return features_.count() == 0;
    }

    public Taxon taxon_with_id(long id) {
        return taxa_.getTaxa_().get(id);
    }

    public Taxon taxon_with_name(String name) {

        if (!this.name2tax_.containsKey(name)) {
            return this.taxa_.getNoTaxon_();
        }
        else {
            int taxonid = this.name2tax_.get(name);

            long tid = this.targets_.get(taxonid).getTax();

            if(this.taxa_.getTaxa_().containsKey(tid)) {
                return this.taxa_.getTaxa_().get(tid);
            }
            else {
                return this.taxa_.getNoTaxon_();
            }

        }

    }

    public Taxon taxon_with_similar_name(String name) {

        if (!this.name2tax_.containsKey(name)) {
            return this.taxa_.getNoTaxon_();
        }
        else {
            String taxonid_str = this.name2tax_.ceilingKey(name);

            if(taxonid_str == null) {
                return this.taxa_.getNoTaxon_();
            }

            if (taxonid_str.contains(name)) {
                int taxonid = this.name2tax_.get(taxonid_str);

                long tid = this.targets_.get(taxonid).getTax();

                if(this.taxa_.getTaxa_().containsKey(tid)) {
                    return this.taxa_.getTaxa_().get(tid);
                }
            }


            return this.taxa_.getNoTaxon_();


        }

    }

    public Long[] ranks(Taxon tax)  {
        return this.taxa_.ranks((Long)tax.getTaxonId());
    }

    public Classification ground_truth(String header) {

        //try to extract query id and find the corresponding target in database
        Taxon tmp_tax = this.taxon_with_name(SequenceReader.extract_ncbi_accession_version_number(header));

        if (tmp_tax != this.taxa_.getNoTaxon_()) {
            return new Classification (tmp_tax.getTaxonId(), tmp_tax);
        }

        tmp_tax = this.taxon_with_similar_name(SequenceReader.extract_ncbi_accession_number(header));

        if (tmp_tax != this.taxa_.getNoTaxon_()) {
            return new Classification (tmp_tax.getTaxonId(), tmp_tax);
        }

        //try to extract id from header
        tmp_tax = this.taxon_with_id(SequenceReader.extract_taxon_id(header));

        if ((tmp_tax != null) && (tmp_tax != this.taxa_.getNoTaxon_())) {
            return new Classification (tmp_tax.getTaxonId(), tmp_tax);
        }

        //try to find entire header as sequence identifier
        tmp_tax = this.taxon_with_name(header);

        if (tmp_tax != this.taxa_.getNoTaxon_()) {
            return new Classification (tmp_tax.getTaxonId(), tmp_tax);
        }

        return new Classification();

    }

    //public Long[] ranks_of_target(long id) {
    //	return targets_.get((int)id).getRanked_lineage();
    //}

    public long[] ranks_of_target_basic(long id) {
/*
		if(targets_.get((int)id) == null) {
			LOG.error("Can not find target id!!: "+id);
			System.exit(-1);
		}
*/
        Long data[] = targets_.get((int)id).getRanked_lineage();

        if(data == null) {
            //LOG.warn("Data is null");
            return null;
        }

        long returnedData[] = new long[data.length];

        for(int i = 0; i< data.length; i++){
            returnedData[i] = data[i].longValue();
            //LOG.warn("JMABUIN: Taxa in target id: "+targets_.get((int)id).getTax()+ " value:" +i+" : " + returnedData[i]);
        }

        return returnedData;
    }

    public Taxon ranked_lca_of_targets(int ta, int tb) {

        //LOG.warn("[JMABUIN] Un: "+ranks_of_target_basic(ta).length+ ", dous: "+ranks_of_target_basic(tb).length);

        return taxa_.lca(ranks_of_target_basic(ta), ranks_of_target_basic(tb));
    }

    public Taxonomy.Rank lowest_common_rank(Classification a, Classification b) {

        if(a.sequence_level() && b.sequence_level() &&
                a.target() == b.target()) return Taxonomy.Rank.Sequence;

        if(a.has_taxon() && b.has_taxon()) {
            return this.ranked_lca(a.tax(), b.tax()).getRank();
        }

        return Taxonomy.Rank.none;
    }

    public Taxon ranked_lca(Taxon ta, Taxon tb) {
        return taxa_.ranked_lca(ta, tb);
    }

    public boolean covers(Taxon t) {
        return covers_taxon(t.getTaxonId());
    }

    public boolean covers_taxon(long id) {

        for(TargetProperty g : targets_) {
            for(long taxid : g.getFull_lineage()) {
                if(taxid == id) return true;
            }
        }
        return false;
    }

    public String sequence_id_of_target(long tid) {
        return targets_.get((int)tid).getIdentifier();
    }

    public SequenceOrigin origin_of_target(int id) {
        return targets_.get(id).getOrigin();
    }

    public Taxon find_taxon_id(String id) {

        if(this.name2tax_.containsKey(id)) {
            long tid = this.targets_.get(this.name2tax_.get(id)).getTax();

            return  this.taxa_.getTaxa_().get(tid);

        }

        String ceiling_key = this.name2tax_.ceilingKey(id);

        if (ceiling_key == null) {
            return this.taxa_.getNoTaxon_();
        }

        //if nearest match contains 'name' as prefix -> good enough
        //e.g. accession vs. accession.version
        if(ceiling_key.contains(id)){
            long tid = this.targets_.get(this.name2tax_.get(ceiling_key)).getTax();

            return  this.taxa_.getTaxa_().get(tid);
        }


        return this.taxa_.getNoTaxon_();

    }

}
