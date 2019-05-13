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

package com.github.jmabuin.metacachespark.database;
import com.github.jmabuin.metacachespark.*;
import com.github.jmabuin.metacachespark.io.*;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import com.github.jmabuin.metacachespark.spark.*;
import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Main class for the Database construction and storage in HDFS (Build mode), and also to load it from HDFS (Query mode)
 * @author Jose M. Abuin
 */
public class Database implements Serializable{

    private static final Log LOG = LogFactory.getLog(Database.class);

    //private Random urbg_;
    //private Sketcher targetSketcher_;
    //private Sketcher querySketcher_;
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
    private HashMap<String,Integer> targets_positions;
    private TreeMap<String, Long> name2tax_;
    private TreeMap<String, Long> name2tax_sequences;
    private Taxonomy taxa_;
    private TaxonomyParam taxonomyParam;
    private int numPartitions = 1;
    private String dbfile;

    private JavaRDD<Sequence> inputSequences;

    private MetaCacheOptions params;
    //private IndexedRDD<Integer, LocationBasic> indexedRDD;
    //private JavaPairRDD<Integer, LocationBasic> locationJavaPairRDD;
    //private JavaPairRDD<Integer, List<LocationBasic>> locationJavaPairListRDD;
    private JavaPairRDD<Integer, Iterable<LocationBasic>> locationJavaPairIterableRDD;
    private JavaRDD<Locations> locationsRDD;
    private JavaRDD<HashMultiMapNative> locationJavaRDDHashMultiMapNative;
    private JavaRDD<Location> locationJavaRDD;
    private JavaRDD<HashMap<Integer, List<LocationBasic>>> locationJavaHashRDD;
    private JavaRDD<HashMultimap<Integer, LocationBasic>> locationJavaHashMMRDD;
    //private RangePartitioner<Integer, LocationBasic> partitioner;
    //private MyCustomPartitioner myPartitioner;

    private JavaSparkContext jsc;
    private SQLContext sqlContext;


    public Database(JavaSparkContext jsc, TaxonomyParam taxonomyParam, MetaCacheOptions params, int numPartitions, String dbfile) {
        this.jsc = jsc;
        this.taxonomyParam = taxonomyParam;
        this.numPartitions = numPartitions;
        this.dbfile = dbfile;

        this.sqlContext = new SQLContext(this.jsc);

        this.targets_ = new ArrayList<TargetProperty>();
        this.name2tax_ = new TreeMap<String,Long>();
        this.name2tax_sequences = new TreeMap<String,Long>();
        this.targets_positions = new HashMap<>();
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

        this.loadFromFile();
        this.readTaxonomy();
        this.readTargets();
        //this.readSid2gid();
        this.readName2tax();

        this.nextTargetId_ = this.name2tax_.size();
        this.apply_taxonomy();

    }
    /*
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
    */
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

    public TreeMap<String, Long> getname2tax_() {
        return name2tax_;
    }

    public void setname2tax_(TreeMap<String, Long> name2tax_) {
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

        for(Map.Entry<Long, Taxon> taxi : this.taxa_.getTaxa_().entrySet()){

            this.name2tax_.put(taxi.getValue().getTaxonName(), taxi.getKey());


        }

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


    public void buildDatabase(String infiles, TreeMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {
            LOG.warn("[buildDatabase] Starting to build database from " + infiles + " ...");

            ArrayList<String> inputDirs = FilesysUtility.directories_in_directory_hdfs(infiles, this.jsc);

            JavaPairRDD<String, String> sequences_files = null;

            int repartition_files = 1;

            if (this.numPartitions == 1) {
                repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);

            }
            else {
                repartition_files = this.numPartitions;
            }

            if (!inputDirs.isEmpty()) {

                /*for(String current_dir: inputDirs) {
                    if (sequences_files == null) {
                        sequences_files = this.jsc.wholeTextFiles(current_dir);
                    }
                    else {
                        sequences_files = sequences_files.union(this.jsc.wholeTextFiles(current_dir));
                    }
                }*/

                sequences_files = this.jsc.wholeTextFiles(infiles+ "/*", repartition_files);


            }
            else {
                sequences_files = this.jsc.wholeTextFiles(infiles, repartition_files);
            }


            if(this.numPartitions == 1) {

                this.inputSequences = sequences_files
                        .flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
                        .persist(StorageLevel.MEMORY_AND_DISK_SER());

            }
            else {
                LOG.warn("Using " +this.numPartitions + " partitions ...");
                this.inputSequences = sequences_files
                        .flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
                        .repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_AND_DISK_SER());
            }


            LOG.info("The number of sequences is: " + this.inputSequences.count());


            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty())
                    .mapToPair(item -> new Tuple2<Integer, TargetProperty>(item.getOrigin().getIndex(), item))
                    .sortByKey(true)
                    .values();

            this.targets_= this.targetPropertiesJavaRDD.collect();

            int i = 0;
            long j = -1;
            for (TargetProperty target: this.targets_) {
                if (target.getTax() == 0) { // Target is unranked. Rank it!
                    target.setTax(j);
                    this.taxa_.getTaxa_().put(j, new Taxon(j, 0, target.getIdentifier(), Taxonomy.Rank.Sequence));
                    --j;
                }
                this.name2tax_sequences.put(target.getIdentifier(), (long)i);
                i++;
            }

/*
			for (TargetProperty target: this.targets_) {

				this.name2tax_.put(target.getIdentifier(),(int) target.getTax());
				this.sid2gid_.put(target.getIdentifier(), i);
				i++;
			}
*/

            LOG.info("Size of name2tax_ is: " + this.name2tax_sequences.size());

            this.nextTargetId_ = this.name2tax_sequences.size();


            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");

                /*this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);
                        */

                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .values()
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");

                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .values()
                        .mapPartitionsWithIndex(new Pair2HashMultiMapGuava(), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");

                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .values()
                        .mapPartitionsWithIndex(new Locations2HashMapNativeIndexed(), true);



            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");
                this.locationJavaRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .values();

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");


                this.locationJavaPairIterableRDD =this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(this.numPartitions))
                        .values()
                        .mapPartitionsToPair(new Location2Pair())
                        .groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }

        } catch (Exception e) {
            LOG.error("ERROR! "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    public void buildDatabaseMultiPartitions(ArrayList<String> infiles, TreeMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {

            this.inputSequences = null;
            //JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;

            int current_target = 0;

            FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

            for (String new_file: infiles) {
                LOG.warn("Adding file " + new_file + " to targets positions...");
                FSDataInputStream is = fs.open(new Path(new_file));

                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                String currentLine;

                while((currentLine = br.readLine()) != null) {

                    if(currentLine.startsWith(">")) {
                        this.targets_positions.put(currentLine.substring(1), current_target);
                        ++current_target;
                    }

                }

                br.close();
                is.close();


            }


            //if(this.params.isMyWholeTextFiles()) {

            JavaRDD<String> tmpInput = this.jsc.parallelize(infiles);

            int repartition_files = 1;

            if (this.numPartitions == 1) {
                repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);

            }
            else {
                repartition_files = this.numPartitions;
            }


            //this.inputSequences = tmpInput
            JavaRDD<Sequence> tmp_sequences = tmpInput
                    .mapPartitions(new Fasta2Sequence(sequ2taxid, this.targets_positions))
                    .repartition(repartition_files)
                    .persist(StorageLevel.MEMORY_AND_DISK());

            //LOG.warn("The total number of input sequences is: " + this.inputSequences.count());
            //LOG.warn("The total number of input sequences is: " + tmp_sequences.count());

            //List<Tuple2<Integer, String>> lenghts_array = this.inputSequences.mapToPair(item -> new Tuple2<Integer, String>(
            List<Tuple2<Integer, String>> lenghts_array = tmp_sequences.mapToPair(item -> new Tuple2<Integer, String>(
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


            this.inputSequences = tmp_sequences
                    .mapToPair(item -> new Tuple2<String, Sequence>(item.getHeader(), item))
                    .partitionBy(new MyCustomPartitionerStr(repartition_files, all_lengths))
                    .values()
                    .persist(StorageLevel.MEMORY_AND_DISK());

            //LOG.warn("The total number of input sorted sequences is: " + this.inputSequences.count());

            tmp_sequences.unpersist();
            tmp_sequences = null;
            //inputData = inputData.coalesce(this.numPartitions);

            //List<SequenceHeaderFilename> data = this.inputSequences.map(new Sequence2HeaderFilename()).collect();

            LOG.info("The number of input sequences is: " + this.inputSequences.count());


            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty())
                    .mapToPair(item -> new Tuple2<Integer, TargetProperty>(item.getOrigin().getIndex(), item))
                    .sortByKey(true)
                    .values();

            this.targets_= this.targetPropertiesJavaRDD.collect();

            i = 0;
            long j = -1;
            for (TargetProperty target: this.targets_) {
                if (target.getTax() == 0) { // Target is unranked. Rank it!
                    target.setTax(j);
                    this.taxa_.getTaxa_().put(j, new Taxon(j, 0, target.getIdentifier(), Taxonomy.Rank.Sequence));
                    --j;
                }
                this.name2tax_sequences.put(target.getIdentifier(), (long)i);
                i++;
            }

            LOG.info("Size of name2tax_ is: " + this.name2tax_sequences.size());

            this.nextTargetId_ = this.name2tax_sequences.size();


            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");

                /*this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(this.params.getPartitions()))
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);
                        */

                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .values()
                        .mapPartitionsWithIndex(new Pair2HashMap(), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");

                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .values()
                        .mapPartitionsWithIndex(new Pair2HashMultiMapGuava(), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");

                /*this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .values()
                        .mapPartitionsWithIndex(new Locations2HashMapNativeIndexed(), true);*/

                this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true);


            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");
                this.locationJavaRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .values();

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");


                this.locationJavaPairIterableRDD =this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .mapPartitionsWithIndex(new Pair2HashMapNative(), true)
                        .mapPartitionsToPair(new HashMapNative2Locations(), true)
                        .partitionBy(new MyCustomPartitioner(repartition_files))
                        .values()
                        .mapPartitionsToPair(new Location2Pair())
                        .groupByKey();

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

    public void buildDatabaseMultiPartitions(String inputdir, TreeMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {
            LOG.warn("Building database in mode buildDatabaseMultiPartitions");
            this.inputSequences = null;
            //JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;

            //this.name2tax_ = sequ2taxid;
            LOG.warn("Number of items in name2tax_ :" + this.name2tax_.size());

            int repartition_files = 1;

            if (this.numPartitions == 1) {
                repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);

            }
            else {
                repartition_files = this.numPartitions;
            }

            int current_target = 0;

            ArrayList<String> infiles = FilesysUtility.files_in_directory(inputdir, 0, this.jsc);

            for(int num = 0; (num < infiles.size()) && (num < 40); ++num) {
                LOG.warn("Assigning file: " + infiles.get(num));
            }


            if (this.params.isMetacache_like_input()) {

                FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

                for (String new_file: infiles) {
                    //LOG.warn("Adding file " + new_file + " to targets positions...");
                    if (!new_file.equals("assembly_summary.txt")) {
                        FSDataInputStream is = fs.open(new Path(new_file));

                        BufferedReader br = new BufferedReader(new InputStreamReader(is));

                        String currentLine;

                        while((currentLine = br.readLine()) != null) {

                            if(currentLine.startsWith(">")) {
                                this.targets_positions.put(currentLine.substring(1), current_target);
                                ++current_target;
                            }

                        }

                        br.close();
                        is.close();

                    }


                }

            }

            else {

                StringBuilder all_files = new StringBuilder();

                LOG.info("Number of files: " + infiles.size());

                for (int index=0; index < infiles.size(); ++index){//String current_file: infiles) {
                    if (!infiles.get(index).equals("assembly_summary.txt")) {

                        if (index < infiles.size()-1) {
                            all_files.append(infiles.get(index) + ",");
                        }
                        else {
                            all_files.append(infiles.get(index));
                        }


                    }
                }

                List<String> sequences = this.jsc.textFile(all_files.toString(), repartition_files)
                        .filter(item -> item.startsWith(">"))
                        .map(item -> item.substring(1))
                        //.repartition(repartition_files)
                        .collect();

                for(String current_header: sequences) {
                    this.targets_positions.put(current_header, current_target);
                    ++current_target;
                }
            }

            //ArrayList<String> infiles = FilesysUtility.files_in_directory(inputdir, 0, this.jsc);
            Map<String, Long> infiles_lengths = Database.sortByComparator(FilesysUtility.files_in_directory_with_size(inputdir,
            //infiles_lengths = Database.sortByComparator(FilesysUtility.files_in_directory_with_size(inputdir,
                    0, this.jsc), false);

            HashMap<String, Integer> partitions_map = new HashMap<String, Integer>();

            int current_partition = 0;
            int partition_pos = 0;

            boolean positive = true;
            for (String current_key: infiles_lengths.keySet()) {
/*
                //if(!current_key.equals("assembly_summary.txt")){
                partitions_map.put(current_key, current_partition);


                current_partition++;

                if (current_partition >= this.numPartitions) {
                    current_partition = 0;
                }
                //}
*/
                partitions_map.put(current_key, current_partition);

                if (partition_pos < this.getNumPartitions()*2 + 3) {
                    LOG.warn("Partitioning file: " + current_key + " into partition: " + current_partition );
                    partition_pos++;
                }

                if(positive) {
                    current_partition++;
                    if (current_partition == this.numPartitions) {
                        positive = false;
                        current_partition-=1;
                    }

                }
                else {
                    current_partition--;
                    if (current_partition == -1) {
                        positive = true;
                        current_partition+=1;
                    }
                }




            }

            long num_files = infiles_lengths.size();

            infiles_lengths.clear();
            infiles_lengths = null;

            JavaRDD<String> tmpInput = this.jsc.parallelize(infiles, repartition_files)
                    .zipWithIndex()
                    .partitionBy(new MyCustomPartitionerStr(this.numPartitions, partitions_map))
                    .keys()
                    .persist(StorageLevel.MEMORY_ONLY_SER());


            //LOG.info("Number of partitioned files: " + tmpInput.count());

            if (!this.params.isRepartition()) {

                JavaPairRDD<String, Long> sequences_lengths_rdd = tmpInput.mapPartitionsToPair(new Fasta2SequenceProperties());

                Map<String, Long> sequences_lengths = Database.sortByComparator(sequences_lengths_rdd
                        .collectAsMap(), false);

                HashMap<Long, Integer> sequences_distribution = new HashMap<>();

                current_partition = 0;
                positive = true;
                partition_pos = 0;
                for (String current_key: sequences_lengths.keySet()) {

                    sequences_distribution.put((long)this.targets_positions.get(current_key), current_partition);

                    if (partition_pos < this.getNumPartitions()*2 + 3) {
                        LOG.warn("Partitioning sequence: " + current_key + " into partition: " + current_partition );
                        partition_pos++;
                    }


                    if(positive) {
                        current_partition++;
                        if (current_partition == this.numPartitions) {
                            positive = false;
                            current_partition-=1;
                        }

                    }
                    else {
                        current_partition--;
                        if (current_partition == -1) {
                            positive = true;
                            current_partition+=1;
                        }
                    }



                }

                this.inputSequences = tmpInput
                        //.keys()
                        .mapPartitionsToPair(new Fasta2SequencePair(sequ2taxid, this.targets_positions))
                        .partitionBy(new MyCustomPartitionerSequenceID(this.numPartitions, sequences_distribution))
                        .values()
                        //.repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_ONLY_SER());
            }
            else {
                this.inputSequences = tmpInput
                        .mapPartitions(new Fasta2Sequence(sequ2taxid, this.targets_positions))
                        .repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_ONLY_SER());
            }


            LOG.warn("Number of input sequences: " + this.inputSequences.count());
            //tmpInput.unpersist();



            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty())
                    .mapToPair(item -> new Tuple2<Integer, TargetProperty>(item.getOrigin().getIndex(), item))
                    .sortByKey(true)
                    .values();

            this.targets_= this.targetPropertiesJavaRDD.collect();

            ExtractionFunctions extraction = new ExtractionFunctions();

            int i = 0;
            long j = -1;
            for (TargetProperty target: this.targets_) {

                //if (target.getTax() == 0) { // Target is unranked. Rank it!

                target.setTax(j);


                String seqId  = extraction.extract_accession_string(target.getHeader());
                String fileId = extraction.extract_accession_string(FilenameUtils.getName(target.getOrigin().getFilename()));

                long parentTaxId = this.find_taxon_id(sequ2taxid, seqId);

                if (parentTaxId == 0) {
                    parentTaxId = this.find_taxon_id(sequ2taxid, fileId);
                }

                if (parentTaxId == 0) {

                    parentTaxId = extraction.extract_taxon_id(target.getHeader());
                }



                this.taxa_.getTaxa_().put(j, new Taxon(j, parentTaxId, target.getIdentifier(), Taxonomy.Rank.Sequence));
                this.name2tax_.put(seqId, j);
                //this.taxa_.getTaxa_().put(j, new Taxon(j, 0, target.getIdentifier(), Taxonomy.Rank.Sequence));
                //--j;
                //}
                this.name2tax_sequences.put(target.getIdentifier(), (long)i);
                i++;
                --j;
            }

            LOG.warn("Size of name2tax_ before ranking is: " + this.name2tax_.size());
            LOG.warn("Size of taxonomy is: " + this.taxa_.getTaxa_().size());
            this.try_to_rank_unranked_targets();

            LOG.warn("Size of name2tax_sequences is: " + this.name2tax_sequences.size());

            this.nextTargetId_ = this.name2tax_sequences.size();


            JavaRDD<HashMultiMapNative> map = this.inputSequences
                    .mapPartitions(new Sketcher2PairPartitions(this.name2tax_sequences, this.params), true)
                    .persist(StorageLevel.MEMORY_ONLY_SER());

            List<Integer> delete_features;

            if (this.params.isRemove_overpopulated_features()) {
                delete_features = map.mapPartitionsToPair(new GetSizes())
                        .reduceByKey(new SizesReducer())
                        .mapPartitions(new SketchesMap2Boolean(this.params))
                        .collect();
            }
            else {
                delete_features = new ArrayList<>();
            }



            //List<Integer> delete_features = new ArrayList<>();


/*
            JavaPairRDD<Integer, LocationBasic> tmp_sketches_rdd = this.inputSequences
                    //.mapPartitionsToPair(new Sketcher2PairPartitions2(this.name2tax_), true)
                    //.mapPartitionsToPair(new Sketcher2PairPartitions2(this.name2tax_sequences, this.params), true)
                    .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                    .persist(StorageLevel.MEMORY_ONLY_SER());

            //LOG.warn("Total items in sketches without overpopulated: " + tmp_sketches_rdd.count());

            List<Integer> delete_features = tmp_sketches_rdd
                    .combineByKey(new CombinerCreate(), new CombinerMerge(), new CombinerMergeCombiners())
                    .mapPartitions(new SketchesMap2Boolean(this.params), true)
                    .collect();

            TreeSet<Integer> delete_features_tree = new TreeSet<>(delete_features);
*/
            //delete_features.clear();

            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");

                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .mapPartitions(new Locations2HashMap2(), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");

                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .mapPartitions(new Locations2HashMultiMapGuava2(), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");

                //this.locationJavaRDDHashMultiMapNative = this.inputSequences
                //        .mapPartitions(new Sketcher2PairPartitions(this.name2tax_sequences, this.params), true);
                //this.locationJavaRDDHashMultiMapNative = tmp_sketches_rdd.mapPartitions(new FilterAndSave(delete_features_tree), true);

                this.locationJavaRDDHashMultiMapNative = map.mapPartitions(new FilterAndSaveNative(delete_features), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");

                this.locationJavaRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .mapPartitions(new Locations2Parquet(), true);

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));


            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");

                this.locationJavaPairIterableRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                        .mapPartitionsToPair(new Pair2Locations(), true)
                        .groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }


        } catch (Exception e) {
            LOG.error("ERROR! "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }



    public void buildDatabaseMultiPartitionsMetacacheLike(String inputdir, TreeMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {

            LOG.warn("Building database in mode buildDatabaseMultiPartitionsMetacacheLike");
            this.inputSequences = null;
            //JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;

            int repartition_files = 1;

            if (this.numPartitions == 1) {
                repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);

            }
            else {
                repartition_files = this.numPartitions;
            }

            int current_target = 0;

            ArrayList<String> infiles = FilesysUtility.files_in_directory(inputdir, 0, this.jsc);


            for(int num = 0; (num < infiles.size()) && (num < 40); ++num) {
                LOG.warn("Assigning file: " + infiles.get(num));
            }

            if (this.params.isMetacache_like_input()) {

                FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

                for (String new_file: infiles) {
                    //LOG.warn("Adding file " + new_file + " to targets positions...");
                    if (!new_file.equals("assembly_summary.txt")) {
                        FSDataInputStream is = fs.open(new Path(new_file));

                        BufferedReader br = new BufferedReader(new InputStreamReader(is));

                        String currentLine;

                        while((currentLine = br.readLine()) != null) {

                            if(currentLine.startsWith(">")) {
                                this.targets_positions.put(currentLine.substring(1), current_target);
                                ++current_target;
                            }

                        }

                        br.close();
                        is.close();

                    }


                }

            }

            else {

                StringBuilder all_files = new StringBuilder();

                LOG.info("Number of files: " + infiles.size());


                for (int index=0; index < infiles.size(); ++index){//String current_file: infiles) {
                    if (!infiles.get(index).equals("assembly_summary.txt")) {

                        if (index < infiles.size()-1) {
                            all_files.append(infiles.get(index) + ",");
                        }
                        else {
                            all_files.append(infiles.get(index));
                        }


                    }
                }

                List<String> sequences = this.jsc.textFile(all_files.toString(), repartition_files)
                        .filter(item -> item.startsWith(">"))
                        .map(item -> item.substring(1))
                        //.repartition(repartition_files)
                        .collect();

                for(String current_header: sequences) {
                    this.targets_positions.put(current_header, current_target);
                    ++current_target;
                }
            }

            //ArrayList<String> infiles = FilesysUtility.files_in_directory(inputdir, 0, this.jsc);
            Map<String, Long> infiles_lengths = Database.sortByComparator(FilesysUtility.files_in_directory_with_size(inputdir,
            //infiles_lengths = Database.sortByComparator(FilesysUtility.files_in_directory_with_size(inputdir,
                    0, this.jsc), false);

            HashMap<String, Integer> partitions_map = new HashMap<String, Integer>();

            int current_partition = 0;
            int partition_pos = 0;

            boolean positive = true;

            for (String current_key: infiles_lengths.keySet()) {
/*
                //if(!current_key.equals("assembly_summary.txt")){
                partitions_map.put(current_key, current_partition);


                current_partition++;

                if (current_partition >= this.numPartitions) {
                    current_partition = 0;
                }
                //}
*/
                partitions_map.put(current_key, current_partition);

                if (partition_pos < this.getNumPartitions()*2 + 3) {
                    LOG.warn("Partitioning file: " + current_key + " into partition: " + current_partition );
                    partition_pos++;
                }

                if(positive) {
                    current_partition++;
                    if (current_partition == this.numPartitions) {
                        positive = false;
                        current_partition-=1;
                    }

                }
                else {
                    current_partition--;
                    if (current_partition == -1) {
                        positive = true;
                        current_partition+=1;
                    }
                }




            }

            infiles_lengths.clear();
            infiles_lengths = null;

            JavaRDD<String> tmpInput = this.jsc.parallelize(infiles, repartition_files)
                    .zipWithIndex()
                    .partitionBy(new MyCustomPartitionerStr(this.numPartitions, partitions_map))
                    .keys()
                    .persist(StorageLevel.MEMORY_ONLY_SER());


            //LOG.info("Number of partitioned files: " + tmpInput.count());

            if (!this.params.isRepartition()) {

                JavaPairRDD<String, Long> sequences_lengths_rdd = tmpInput.mapPartitionsToPair(new Fasta2SequenceProperties());

                Map<String, Long> sequences_lengths = Database.sortByComparator(sequences_lengths_rdd
                        .collectAsMap(), false);

                HashMap<Long, Integer> sequences_distribution = new HashMap<>();

                current_partition = 0;
                positive = true;
                partition_pos = 0;
                for (String current_key: sequences_lengths.keySet()) {

                    sequences_distribution.put((long)this.targets_positions.get(current_key), current_partition);

                    if (partition_pos < this.getNumPartitions()*2 + 3) {
                        LOG.warn("Partitioning sequence: " + current_key + " into partition: " + current_partition );
                        partition_pos++;
                    }


                    if(positive) {
                        current_partition++;
                        if (current_partition == this.numPartitions) {
                            positive = false;
                            current_partition-=1;
                        }

                    }
                    else {
                        current_partition--;
                        if (current_partition == -1) {
                            positive = true;
                            current_partition+=1;
                        }
                    }



                }

                this.inputSequences = tmpInput
                        //.keys()
                        .mapPartitionsToPair(new Fasta2SequencePair(sequ2taxid, this.targets_positions))
                        .partitionBy(new MyCustomPartitionerSequenceID(this.numPartitions, sequences_distribution))
                        .values()
                        //.repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_ONLY_SER());
            }
            else {
                this.inputSequences = tmpInput
                        .mapPartitions(new Fasta2Sequence(sequ2taxid, this.targets_positions))
                        .repartition(this.numPartitions)
                        .persist(StorageLevel.MEMORY_ONLY_SER());
            }

            LOG.warn("Number of input sequences: " + this.inputSequences.count());
            //tmpInput.unpersist();



            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty())
                    .mapToPair(item -> new Tuple2<Integer, TargetProperty>(item.getOrigin().getIndex(), item))
                    .sortByKey(true)
                    .values();

            this.targets_= this.targetPropertiesJavaRDD.collect();

            ExtractionFunctions extraction = new ExtractionFunctions();

            int i = 0;
            long j = -1;
            for (TargetProperty target: this.targets_) {
                //if (target.getTax() == 0) { // Target is unranked. Rank it!

                target.setTax(j);


                String seqId  = extraction.extract_accession_string(target.getHeader());
                String fileId = extraction.extract_accession_string(FilenameUtils.getName(target.getOrigin().getFilename()));

                long parentTaxId = this.find_taxon_id(sequ2taxid, seqId);

                if (parentTaxId == 0) {
                    parentTaxId = this.find_taxon_id(sequ2taxid, fileId);
                }

                if (parentTaxId == 0) {

                    parentTaxId = extraction.extract_taxon_id(target.getHeader());
                }



                this.taxa_.getTaxa_().put(j, new Taxon(j, parentTaxId, target.getIdentifier(), Taxonomy.Rank.Sequence));

                this.name2tax_.put(target.getIdentifier(), j);
                //this.taxa_.getTaxa_().put(j, new Taxon(j, 0, target.getIdentifier(), Taxonomy.Rank.Sequence));
                //--j;
                //}
                this.name2tax_sequences.put(target.getIdentifier(), (long)i);
                i++;
                --j;
            }

            LOG.warn("Size of name2tax_ before ranking is: " + this.name2tax_.size());
            LOG.warn("Size of taxonomy is: " + this.taxa_.getTaxa_().size());
            this.try_to_rank_unranked_targets();


            LOG.warn("Size of name2tax_sequences is: " + this.name2tax_sequences.size());

            this.nextTargetId_ = this.name2tax_sequences.size();

            JavaPairRDD<Integer, LocationBasic> tmp_rdd = this.inputSequences
                    //.mapPartitionsToPair(new Sketcher2PairPartitions2(this.name2tax_), true)
                    .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                    .partitionBy(new MyCustomPartitioner(this.numPartitions))
                    .mapPartitionsToPair(new Pair2KeyValues(this.params))
                    .partitionBy(new MyCustomPartitioner(this.numPartitions))
                    .persist(StorageLevel.MEMORY_ONLY_SER());

/*
            JavaPairRDD<Integer, LocationBasic> tmp_sketches_rdd = this.inputSequences
                    //.mapPartitionsToPair(new Sketcher2PairPartitions2(this.name2tax_), true)
                    .flatMapToPair(new Sketcher2Pair(this.name2tax_sequences))
                    .persist(StorageLevel.MEMORY_ONLY_SER());

            Map<Integer, Integer> sketches_sizes = tmp_sketches_rdd.mapToPair(new SketchesMap())
                    .reduceByKey(new SketchesReducer())
                    .collectAsMap();

            Map<Integer, Boolean> delete_features = new HashMap<>();

            for(Map.Entry<Integer, Integer> entry : sketches_sizes.entrySet()) {

                if (entry.getValue() > this.params.getProperties().getMax_locations_per_feature()) {
                    delete_features.put(entry.getValue(), true);
                }
                else {
                    delete_features.put(entry.getValue(), false);
                }

            }

            sketches_sizes.clear();
*/




            LOG.warn("Number of items in HashMap: " + tmp_rdd.count());

            LOG.warn("Building database with isBuildModeHashMultiMapMC");

            this.locationJavaRDDHashMultiMapNative = tmp_rdd
                    .mapPartitions(new Locations2HashMapNative(), true);
                    //.mapPartitions(new Locations2HashMapNativeIterable(this.params), true);

/*
            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");

                this.locationJavaHashRDD = tmp_rdd
                        .mapPartitions(new Locations2HashMap(), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");

                this.locationJavaHashMMRDD = tmp_rdd
                        .mapPartitions(new Locations2HashMultiMapGuava(), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
            LOG.warn("Building database with isBuildModeHashMultiMapMC");

            this.locationJavaRDDHashMultiMapNative = tmp_rdd
                    .mapPartitions(new Locations2HashMapNative(), true);
                    //.mapPartitions(new Locations2HashMapNativeIterable(), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");

                this.locationJavaRDD = tmp_rdd
                        .mapPartitions(new Locations2Parquet(), true);

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");

                this.locationJavaPairIterableRDD = tmp_rdd
                        .groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }
*/

        } catch (Exception e) {
            LOG.error("ERROR! "+e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    public void buildDatabaseSimple(String inputdir, TreeMap<String, Long> sequ2taxid, Build.build_info infoMode) {
        try {

            this.inputSequences = null;
            //JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;

            int repartition_files = 1;

            if (this.numPartitions == 1) {
                repartition_files = this.jsc.getConf().getInt("spark.executor.instances", 1);

            }
            else {
                repartition_files = this.numPartitions;
            }

            int current_target = 0;


            ArrayList<String> infiles = FilesysUtility.files_in_directory(inputdir, 0, this.jsc);



            StringBuilder all_files = new StringBuilder();

            LOG.info("Number of files: " + infiles.size());

            for (int index=0; index < infiles.size(); ++index){//String current_file: infiles) {
                if (!infiles.get(index).equals("assembly_summary.txt")) {

                    if (index < infiles.size()-1) {
                        all_files.append(infiles.get(index) + ",");
                    }
                    else {
                        all_files.append(infiles.get(index));
                    }


                }
            }

            List<String> sequences = this.jsc.textFile(all_files.toString(), repartition_files)
                    .filter(item -> item.startsWith(">"))
                    .map(item -> item.substring(1))
                    .collect();

            for(String current_header: sequences) {
                this.targets_positions.put(current_header, current_target);
                ++current_target;
            }

            JavaPairRDD<String, Long> tmpInput = this.jsc.parallelize(infiles, repartition_files)
                    .zipWithIndex()
                    //.partitionBy(new MyCustomPartitionerStr(this.numPartitions, partitions_map))
                    .persist(StorageLevel.MEMORY_ONLY_SER());


            this.inputSequences = tmpInput
                    .keys()
                    .mapPartitionsToPair(new Fasta2SequencePairNaive(sequ2taxid, this.targets_positions), true)
                    //.partitionBy(new MyCustomPartitionerSequenceID(this.numPartitions, sequences_distribution))
                    .values()
                    .persist(StorageLevel.MEMORY_ONLY_SER());

            LOG.warn("Number of input sequences: " + this.inputSequences.count());
            tmpInput.unpersist();

            this.targetPropertiesJavaRDD = this.inputSequences
                    .map(new Sequence2TargetProperty())
                    .mapToPair(item -> new Tuple2<Integer, TargetProperty>(item.getOrigin().getIndex(), item))
                    .sortByKey(true)
                    .values();

            this.targets_= this.targetPropertiesJavaRDD.collect();

            ExtractionFunctions extraction = new ExtractionFunctions();

            int i = 0;
            long j = -1;
            for (TargetProperty target: this.targets_) {

                //if (target.getTax() == 0) { // Target is unranked. Rank it!

                target.setTax(j);


                String seqId  = extraction.extract_accession_string(target.getHeader());
                String fileId = extraction.extract_accession_string(FilenameUtils.getName(target.getOrigin().getFilename()));

                long parentTaxId = this.find_taxon_id(sequ2taxid, seqId);

                if (parentTaxId == 0) {
                    parentTaxId = this.find_taxon_id(sequ2taxid, fileId);
                }

                if (parentTaxId == 0) {

                    parentTaxId = extraction.extract_taxon_id(target.getHeader());
                }



                this.taxa_.getTaxa_().put(j, new Taxon(j, parentTaxId, target.getIdentifier(), Taxonomy.Rank.Sequence));

                //this.taxa_.getTaxa_().put(j, new Taxon(j, 0, target.getIdentifier(), Taxonomy.Rank.Sequence));
                //--j;
                //}
                this.name2tax_.put(target.getIdentifier(), (long)i);
                i++;
                --j;
            }

            LOG.info("Size of name2tax_ is: " + this.name2tax_.size());

            this.nextTargetId_ = this.name2tax_.size();

            List<Integer> executors = new ArrayList<>();

            for(i = 0; i< this.numPartitions; ++i) {

                LOG.warn("Adding virtual executor: " + i);
                executors.add(i);

            }


            JavaRDD<Integer> executors_rdd = this.jsc.parallelize(executors, this.numPartitions);


            if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMAP) {//(paramsBuild.isBuildModeHashMap()) {
                LOG.warn("Building database with isBuildModeHashMap");

                this.locationJavaHashRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .mapPartitions(new Locations2HashMap2(), true);

            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_GUAVA) { //(paramsBuild.isBuildModeHashMultiMapG()) {
                LOG.warn("Building database with isBuildModeHashMultiMapG");

                this.locationJavaHashMMRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .mapPartitions(new Locations2HashMultiMapGuava2(), true);
            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.HASHMULTIMAP_NATIVE) {//(paramsBuild.isBuildModeHashMultiMapMC()) {
                LOG.warn("Building database with isBuildModeHashMultiMapMC");

                /*this.locationJavaRDDHashMultiMapNative = this.inputSequences
                        .mapPartitions(new Sketcher2PairPartitions(this.name2tax_), true);
*/
                this.locationJavaRDDHashMultiMapNative = executors_rdd
                        .mapPartitions(new CreateDatabaseNative(infiles, sequ2taxid, this.name2tax_, this.targets_positions, this.numPartitions),
                                true);


            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {//(paramsBuild.isBuildModeParquetDataframe()) {
                LOG.warn("Building database with isBuildModeParquetDataframe");

                this.locationJavaRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .mapPartitions(new Locations2Parquet(), true);

                this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class));


            }
            else if (this.params.getDatabase_type() == EnumModes.DatabaseType.COMBINE_BY_KEY) { //(paramsBuild.isBuildCombineByKey()) {
                LOG.warn("Building database with isBuildCombineByKey");

                this.locationJavaPairIterableRDD = this.inputSequences
                        .flatMapToPair(new Sketcher2Pair(this.name2tax_))
                        .mapPartitionsToPair(new Pair2Locations(), true)
                        .groupByKey();

                this.locationsRDD = this.locationJavaPairIterableRDD.map(new LocationKeyIterable2Locations(
                        this.params.getProperties().getMax_locations_per_feature()));

            }


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
            this.name2tax_ = (TreeMap<String, Long>) ois.readObject();

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

            for(Map.Entry<String, Long> currentEntry: this.name2tax_.entrySet()) {

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
                this.name2tax_ = new TreeMap<String, Long>();
            }

            // read data
            long numberItems = Long.parseLong(br.readLine());

            String currentLine;

            while((currentLine = br.readLine()) != null) {

                String parts[] = currentLine.split(":");

                this.name2tax_.put(parts[0], Long.parseLong(parts[1]));

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
    public TreeMap<String, Long> make_sequence_to_taxon_id_map(ArrayList<String> mappingFilenames,ArrayList<String> infilenames) {
        //HashMap<String, Long> make_sequence_to_taxon_id_map(ArrayList<String> mappingFilenames,String infilenames)	{
        //gather all taxonomic mapping files that can be found in any
        //of the input directories

        TreeMap<String, Long> map = new TreeMap<String, Long>();

        //String dir = infilenames.get(0);

        //for(String newFile: mappingFilenames) {
        for(String newFile: infilenames) {
            //System.err.println("[JMAbuin] Accessing file: " + newFile + " in make_sequence_to_taxon_id_map");
            read_sequence_to_taxon_id_mapping(newFile, map);
        }

        return map;

    }

    public void read_sequence_to_taxon_id_mapping(String mappingFile, TreeMap<String, Long> map){


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


    public Set<Long> unranked_targets() {
        Set<Long> res = new HashSet<Long>();

        for( long i = 0; i < this.targets_.size(); ++i) {

            if(this.taxa_.getTaxa_().get(this.targets_.get((int)i).getTax()).getParentId() == 0) {
                res.add(this.targets_.get((int)i).getTax());
            }

        }

        LOG.warn("Number of unranked trgets is: " + res.size());

        /*for(long i = 0; i < this.target_count(); ++i) {
            if(this.taxon_of_target(i) == null) {
                res.add(i);
            }
        }*/

        return res;
    }

    public Long target_id_of_sequence(String sid) {
        //String it = name2tax_..find(sid);

        if(this.name2tax_.containsKey(sid)) {
            return this.name2tax_.get(sid);
        }
        else {
            return (long)nextTargetId_;
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

    public void try_to_rank_unranked_targets() {
        Set<Long> unranked = this.unranked_targets();

        if(!unranked.isEmpty()) {
            LOG.warn(unranked.size() + " targets could not be ranked.");

            for(String file : this.taxonomyParam.getMappingPostFiles()) {
                //this.rank_targets_post_process(unranked, file);
                this.rank_targets_with_mapping_file(file, unranked);
            }
        }

        unranked = this.unranked_targets();

        if(unranked.isEmpty()) {
            LOG.warn("All targets are ranked.");
        }
        else {
            LOG.warn(unranked.size() + " targets remain unranked.");
            int i = 0;

            Iterator<Long> it_unranked = unranked.iterator();



            for (i = 0; i< 40 && it_unranked.hasNext(); ++i) {
                Long current = it_unranked.next();

                Long id = this.getTaxa_().getTaxa_().get(current).getTaxonId();
                String name = this.getTaxa_().getTaxa_().get(current).getTaxonName();

                LOG.warn("Unranked taxon: " + id + " :: " + name);

            }
        }

    }

    /*************************************************************************//**
     *
     * @brief Alternative way of providing taxonomic mappings.
     *        The input file must be a text file with each line in the format:
     *        accession  accession.version taxid gi
     *        (like the NCBI's *.accession2version files)
     *
     *****************************************************************************/
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
                        int tid = this.target_id_of_sequence(accver).intValue();

                        if(!this.is_valid(tid)) {
                            tid = this.target_id_of_sequence(acc).intValue();
                            if(!this.is_valid(tid)) {
                                tid = this.target_id_of_sequence(gi).intValue();
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

                List<String> outputs = this.locationJavaRDDHashMultiMapNative
                        .mapPartitionsWithIndex(new WriteHashMapNative(path+"/"+this.dbfile), true)
                        .collect();

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

            long init_time = System.nanoTime();

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
                    .persist(StorageLevel.MEMORY_ONLY());


            LOG.warn("The number of paired persisted entries is: " + this.locationJavaRDDHashMultiMapNative.count());

            long end_time = System.nanoTime();

            LOG.warn("[QUERY] Time spent in loading database for " + this.params.getOutfile() + " from HDFS is: " + ((end_time - init_time) / 1e9) + " seconds");
        }
        else if(this.params.getDatabase_type() == EnumModes.DatabaseType.PARQUET) {
            LOG.warn("Loading database with isBuildModeParquetDataframe");
            //DataFrame dataFrame = this.sqlContext.read().parquet(this.dbfile);
            Dataset<Location> dataFrame = this.sqlContext.read().parquet(this.dbfile).map(new Row2Location(), Encoders.bean(Location.class));

            this.locationJavaRDD = dataFrame.javaRDD();

            this.featuresDataframe_ = this.sqlContext.createDataset(this.locationJavaRDD.rdd(), Encoders.bean(Location.class))
                    .persist(StorageLevel.MEMORY_AND_DISK());

            this.featuresDataframe_.createOrReplaceTempView("MetaCacheSpark");
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



/*    public Map<Long, List<MatchCandidate>> accumulate_matches_native_buffered_best( String fileName, String fileName2,
                                                                              long init, int size) {

        long initTime = System.nanoTime();

        Map<Long, List<MatchCandidate>> results = this.locationJavaRDDHashMultiMapNative
                .mapPartitionsToPair(new PartialQueryNativePaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                .reduceByKey(new QueryReducerListNative(this.params))
                .collectAsMap();
        long endTime = System.nanoTime();

        LOG.warn("Time in insert into TreeMap partial is: " + ((endTime - initTime) / 1e9) + " seconds");

        return results;


    }
*/

    public Map<Long, List<MatchCandidate>> accumulate_matches_paired_full(String fileName, String fileName2,
                                                                     long init, int size) {

        Map<Long, List<MatchCandidate>> results = null;

        //CandidateGenerationRules rules = new CandidateGenerationRules(this.params.getProperties());

        if (this.params.getNumThreads() == 1) {
            switch (this.params.getDatabase_type()) {

                case HASHMULTIMAP_NATIVE:
                    //LOG.warn("Yes, it is full");
                    results = this.locationJavaRDDHashMultiMapNative
                            .mapPartitionsToPair(new PartialQueryNativePairedFull(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNativeFull(this.params))
                            .collectAsMap();

                    break;
                case HASHMAP:
                    results = this.locationJavaHashRDD
                            .mapPartitionsToPair(new PartialQueryJavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                case HASHMULTIMAP_GUAVA:
                    results = this.locationJavaHashMMRDD
                            .mapPartitionsToPair(new PartialQueryGuavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                default:
                    //this.classify(filename, d, stats);
                    break;


            }
        }
        else {
            switch (this.params.getDatabase_type()) {

                case HASHMULTIMAP_NATIVE:
                    results = this.locationJavaRDDHashMultiMapNative
                            .mapPartitionsToPair(new PartialQueryNativeMultiThreadPairedFull(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNativeFull(this.params))
                            .collectAsMap();

                    break;
                case HASHMAP:
                    results = this.locationJavaHashRDD
                            .mapPartitionsToPair(new PartialQueryJavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                case HASHMULTIMAP_GUAVA:
                    results = this.locationJavaHashMMRDD
                            .mapPartitionsToPair(new PartialQueryGuavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                default:
                    //this.classify(filename, d, stats);
                    break;


            }
        }
        return results;
    }



    public Map<Long, List<MatchCandidate>> accumulate_matches_paired(String fileName, String fileName2,
                                                                     long init, int size) {

        Map<Long, List<MatchCandidate>> results = null;

        //CandidateGenerationRules rules = new CandidateGenerationRules(this.params.getProperties());

        if (this.params.getNumThreads() == 1) {
            switch (this.params.getDatabase_type()) {

                case HASHMULTIMAP_NATIVE:
                    results = this.locationJavaRDDHashMultiMapNative
                            .mapPartitionsToPair(new PartialQueryNativePaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();

                    break;
                case HASHMAP:
                    results = this.locationJavaHashRDD
                            .mapPartitionsToPair(new PartialQueryJavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                case HASHMULTIMAP_GUAVA:
                    results = this.locationJavaHashMMRDD
                            .mapPartitionsToPair(new PartialQueryGuavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                default:
                    //this.classify(filename, d, stats);
                    break;


            }
        }
        else {
            switch (this.params.getDatabase_type()) {

                case HASHMULTIMAP_NATIVE:
                    results = this.locationJavaRDDHashMultiMapNative
                            .mapPartitionsToPair(new PartialQueryNativeMultiThreadPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();

                    break;
                case HASHMAP:
                    results = this.locationJavaHashRDD
                            .mapPartitionsToPair(new PartialQueryJavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                case HASHMULTIMAP_GUAVA:
                    results = this.locationJavaHashMMRDD
                            .mapPartitionsToPair(new PartialQueryGuavaPaired(fileName, fileName2, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                default:
                    //this.classify(filename, d, stats);
                    break;


            }
        }
        return results;
    }

    public Map<Long, List<MatchCandidate>> accumulate_matches_single(String fileName,
                                                                     long init, int size) {

        Map<Long, List<MatchCandidate>> results = null;

        if (this.params.getNumThreads() == 1) {
            switch (this.params.getDatabase_type()) {

                case HASHMULTIMAP_NATIVE:
                    results = this.locationJavaRDDHashMultiMapNative
                            .mapPartitionsToPair(new PartialQueryNative(fileName, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();

                    break;
                case HASHMAP:
                    results = this.locationJavaHashRDD
                            .mapPartitionsToPair(new PartialQueryJava(fileName, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;

                case HASHMULTIMAP_GUAVA:
                    results = this.locationJavaHashMMRDD
                            .mapPartitionsToPair(new PartialQueryGuava(fileName, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                case PARQUET:
                    break;

                case COMBINE_BY_KEY:
                    break;
                default:
                    //this.classify(filename, d, stats);
                    break;


            }
        }
        else {
            switch (this.params.getDatabase_type()) {

                case HASHMULTIMAP_NATIVE:
                    results = this.locationJavaRDDHashMultiMapNative
                            .mapPartitionsToPair(new PartialQueryNativeMultiThread(fileName, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();

                    break;
                case HASHMAP:
                    results = this.locationJavaHashRDD
                            .mapPartitionsToPair(new PartialQueryJava(fileName, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;

                case HASHMULTIMAP_GUAVA:
                    results = this.locationJavaHashMMRDD
                            .mapPartitionsToPair(new PartialQueryGuava(fileName, init, size, this.getTargetWindowStride_(), this.params), true)
                            .reduceByKey(new QueryReducerListNative(this.params))
                            .collectAsMap();
                    break;
                case PARQUET:
                    break;

                case COMBINE_BY_KEY:
                    break;
                default:
                    //this.classify(filename, d, stats);
                    break;


            }
        }



        return results;
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
        /*
        if(name.empty()) return nullptr;
        auto i = name2tax_.find(name);
        if(i == name2tax_.end()) return nullptr;
        return i->second;
         */

        if (name.isEmpty()) {
            return this.taxa_.getNoTaxon_();
        }

        if (!this.name2tax_.containsKey(name)) {
            return this.taxa_.getNoTaxon_();
        }
        else {
            long taxonid = this.name2tax_.get(name);

            //long tid = this.targets_.get(taxonid).getTax();

            //if(this.taxa_.getTaxa_().containsKey(tid)) {
                return this.taxa_.getTaxa_().get(taxonid);
            //}
            //else {
            //    return this.taxa_.getNoTaxon_();
            //}

        }

    }

    public Taxon taxon_with_similar_name(String name) {

        /*
        if(name.empty()) return nullptr;
        auto i = name2tax_.upper_bound(name);
        if(i == name2tax_.end()) return nullptr;
        const auto s = name.size();
        if(0 != i->first.compare(0,s,name)) return nullptr;
        return i->second;
         */

        if (name.isEmpty()) {
            return this.taxa_.getNoTaxon_();
        }

        String taxonid_str = this.name2tax_.higherKey(name);

        if(taxonid_str == null) {
            return this.taxa_.getNoTaxon_();
        }

        if (! taxonid_str.startsWith(name)) {
            return this.taxa_.getNoTaxon_();
        }

        long taxonid = this.name2tax_.get(taxonid_str);

        //long tid = this.targets_.get(taxonid).getTax();

        //if(this.taxa_.getTaxa_().containsKey(tid)) {
        return this.taxa_.getTaxa_().get(taxonid);
        //}

    }

    /*************************************************************************//**
     *
     * @brief Alternative way of providing taxonomic mappings.
     *        The input file must be a text file with each line in the format:
     *        accession  accession.version taxid gi
     *        (like the NCBI's *.accession2version files)
     *
     *****************************************************************************/
    void rank_targets_with_mapping_file(String mappingFile, Set<Long> targetTaxa)  {

        if(targetTaxa.isEmpty()) return;


        try {
            LOG.warn("Try to map sequences to taxa using '" + mappingFile);

            //JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());
            FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

            Path current_file_path = new Path(mappingFile);

            if(!fs.isFile(current_file_path)) {
                LOG.warn("File: " + mappingFile + " does not exist");
                return;
            }

            Path local_file = new Path(FilenameUtils.getName(mappingFile));

            fs.copyToLocalFile(current_file_path, local_file);

            //FSDataInputStream inputStream = fs.open(new Path(mappingFile));
            //BufferedReader d = new BufferedReader(new InputStreamReader(inputStream));


            FileReader file_Reader = new FileReader(local_file.getName());

            BufferedReader d = new BufferedReader(file_Reader);

            String acc;
            String accver;
            Long taxid;
            String gi;


            String newLine = d.readLine();
            LOG.warn("First line: " + newLine);

            while((newLine = d.readLine()) != null) {

                //skip header
                if(newLine.startsWith("#")) {
                    LOG.warn("Skipping header");
                }
                else {

                    String lineSplits[] = newLine.split("\t");

                    if(lineSplits.length == 4) {

                        acc = lineSplits[0].trim();
                        accver = lineSplits[1].trim();
                        taxid = Long.valueOf(lineSplits[2]);
                        gi = lineSplits[3].trim();


                        /*if (acc.contains("CM000378")) {
                            LOG.warn(acc + " :: " + accver + " :: " + taxid + " :: " + gi);
                            if (this.name2tax_.containsKey(acc)) {
                                LOG.warn("name2tax contains " + acc);
                            }
                            else {
                                LOG.warn("name2tax does not contains " + acc);
                            }
                        }*/
                        //LOG.warn(acc + " :: " + accver + " :: " + taxid + " :: " + gi);
                        //target in database?
                        //accession.version is the default
                        Taxon tax = this.taxon_with_name(accver);


                        if( (tax == null) || tax.equals(this.getTaxa_().getNoTaxon_())) {
                            tax = this.taxon_with_similar_name(acc);
                            if(tax.equals(this.getTaxa_().getNoTaxon_())) {
                                tax = this.taxon_with_name(gi);
                                /*if (tax.equals(this.getTaxa_().getNoTaxon_()) && (acc.contains("CM000378"))) {
                                    LOG.warn("Could not be ranked!");
                                }
                                else if (acc.contains("CM000378")) {
                                    LOG.warn("Found by taxon_with_name(gi): " + accver);
                                }*/
                            }
                            /*else if (acc.contains("CM000378")) {
                                LOG.warn("Found by taxon_with_similar_name: " + accver);
                            }*/
                        }
                        /*
                        else if (acc.contains("CM000378")) {
                            LOG.warn("Found by taxon_with_name: " + accver);
                        }*/

                        //if in database then set parent
                        if(!tax.equals(this.getTaxa_().getNoTaxon_())) {

                            if (targetTaxa.contains(tax.getTaxonId())){

                                //LOG.warn("Setting parent for " + tax.getTaxonId() + "::"+tax.getTaxonName()+" parent is " + taxid);
                                //this.taxa_.getTaxa_().put(taxid, new Taxon())
                                this.taxa_.getTaxa_().get(tax.getTaxonId()).setParentId(taxid);
                                targetTaxa.remove(tax.getTaxonId());
                                if (targetTaxa.isEmpty()) {
                                    break;
                                }

                            }
                            /*else {
                                LOG.warn("Not in database: " + accver + ", taxid:" + taxid);
                            }*/

                        }


                        //i+=4;
                    }
                    else {
                        LOG.warn("Bad line in file " + mappingFile + " :: " + newLine);
                    }

                }

            }

            d.close();
            file_Reader.close();
            //inputStream.close();
            //fs.close();
        }

        catch (IOException e) {
            LOG.error("I/O Error accessing HDFS in rank_targets_with_mapping_file: "+e.getMessage());
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("General error accessing HDFS in rank_targets_with_mapping_file: "+e.getMessage());
            //System.exit(1);
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


    /*

    taxon_id find_taxon_id(
    const std::map<string,taxon_id>& name2tax,
    const string& name)
{
    if(name2tax.empty()) return taxonomy::none_id();
    if(name.empty()) return taxonomy::none_id();

    //try to find exact match
    auto i = name2tax.find(name);
    if(i != name2tax.end()) return i->second;

    //find nearest match
    i = name2tax.upper_bound(name);
    if(i == name2tax.end()) return taxonomy::none_id();

    //if nearest match contains 'name' as prefix -> good enough
    //e.g. accession vs. accession.version
    if(i->first.compare(0,name.size(),name) != 0) return taxonomy::none_id();
    return i->second;
}

     */

    public long find_taxon_id(TreeMap<String, Long> sequ2taxid, String id) {

        //try to find exact match
        if(sequ2taxid.containsKey(id)) {

            return sequ2taxid.get(id);

        }

        //find nearest match
        String ceiling_key = sequ2taxid.ceilingKey(id);

        if (ceiling_key == null) {
            return 0;
        }

        //if nearest match contains 'name' as prefix -> good enough
        //e.g. accession vs. accession.version
        if(ceiling_key.contains(id)){
            return sequ2taxid.get(ceiling_key);
        }


        return 0;

    }

    private static Map<String, Long> sortByComparator(Map<String, Long> unsortMap, final boolean order)
    {

        List<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String, Long>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>()
        {
            public int compare(Map.Entry<String, Long> o1,
                               Map.Entry<String, Long> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();

        for (Map.Entry<String, Long> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }


    public void estimate_abundance(ConcurrentSkipListMap<Taxon, Float> allTaxCounts, Taxonomy.Rank rank) {

/*
        for(Taxon taxCount: allTaxCounts.keySet()) {
            LOG.warn("Taxon in allTaxCounts: " + taxCount.rank_name()+ ":" + taxCount.getTaxonId() + ":" + taxCount.getTaxonName() + " :: " + allTaxCounts.get(taxCount));
        }
*/

        List<Taxon> mark_for_removal = new ArrayList<Taxon>();
        if (rank.ordinal() != Taxonomy.Rank.Sequence.ordinal()) {

            //prune taxon below estimation rank
            Taxon t = new Taxon(0, 0, "", rank.previous());

            Taxon begin = allTaxCounts.ceilingKey(t);

            //LOG.warn("Ceiling key is: " + begin.getTaxonName() + " with ID: " + begin.getTaxonId());
            boolean start = false;


            //for(Taxon taxCount = begin; taxCount != allTaxCounts.end();) {
            Set<Taxon> keys = allTaxCounts.keySet();
            for (Taxon taxCount : keys) {

                if (taxCount == begin) {
                    start = true;
                }

                if (start) {
                    Long[] lineage = this.ranks(taxCount);
                    Taxon ancestor = null;
                    int index = rank.ordinal();

                    //unsigned index = static_cast<unsigned>(rank);

                    while ((ancestor == null) && (index < (lineage.length))) {
                        ancestor = this.taxa_.getTaxa_().get(lineage[index]);
                        index +=1;
                    }

                    if ((ancestor != null) && (ancestor != this.taxa_.getNoTaxon_()) && (ancestor.getRank().ordinal() > taxCount.getRank().ordinal())) {

                        if (!allTaxCounts.containsKey(ancestor)) {
                            allTaxCounts.put(ancestor, 0F);
                        }

                        allTaxCounts.put(ancestor, allTaxCounts.get(ancestor) + allTaxCounts.get(taxCount));
                        allTaxCounts.remove(taxCount);
                        //mark_for_removal.add(taxCount);

                    }

                }

                /*for(Taxon item: mark_for_removal) {
                    allTaxCounts.remove(item);
                }

                mark_for_removal.clear();
                */

            }
        }

        LOG.warn("After first loop");

        for(Taxon taxCount: allTaxCounts.keySet()) {
            LOG.warn("Taxon in allTaxCounts: " + taxCount.rank_name()+ ":" + taxCount.getTaxonId() + ":" + taxCount.getTaxonName() + " :: " + allTaxCounts.get(taxCount));
        }

        HashMap<Taxon, List<Taxon>> taxChildren = new HashMap<>();
        HashMap<Taxon, Long> taxWeights = new HashMap<>(allTaxCounts.size());

        //initialize weigths for fast lookup
        for (Taxon key : allTaxCounts.keySet()) {
            taxWeights.put(key, 0L);
        }

        //for every taxon find its parent and add to their count
        //traverse allTaxCounts from leafs to root
        ArrayList<Taxon> keyList = new ArrayList<Taxon>(allTaxCounts.keySet());

        for (int i = keyList.size() - 1; i >= 0; --i) {
            Taxon taxCount = keyList.get(i);

            //find closest parent
            Long[] lineage = this.ranks(taxCount);
            Taxon parent = null;

            int index = taxCount.getRank().ordinal() + 1;

            while (index < lineage.length) {
                parent = this.taxa_.getTaxa_().get(lineage[index]);

                if ((lineage[index] != 0) && (parent != null) && (parent != this.taxa_.getNoTaxon_()) && taxWeights.containsKey(parent)) {
                    //add own count to parent
                    taxWeights.put(parent, (long) (taxWeights.get(parent) + taxWeights.get(taxCount) + allTaxCounts.get(taxCount)));
                    //link from parent to child
                    if (!taxChildren.containsKey(parent)) {
                        taxChildren.put(parent, new ArrayList<Taxon>());
                    }
                    taxChildren.get(parent).add(taxCount);
                    break;
                }

                index += 1;

            }
        }

        for(Taxon taxCount: taxChildren.keySet()) {
            LOG.warn("Taxon in taxChildren: " + taxCount.rank_name()+ ":" + taxCount.getTaxonId() + ":" + taxCount.getTaxonName() + " :: " + allTaxCounts.get(taxCount));
        }


        //distribute counts to children and erase parents
        //traverse allTaxCounts from root to leafs
        for (Taxon taxCount : allTaxCounts.keySet()) {

            if (taxChildren.containsKey(taxCount)) {
                //Taxon children = taxChildren.get(taxCount);

                long sumChildren = taxWeights.get(taxCount);

                //distribute proportionally
                for (Taxon child : taxChildren.get(taxCount)) {
                    float result = allTaxCounts.get(child) + allTaxCounts.get(taxCount) * (allTaxCounts.get(child) + taxWeights.get(child)) / sumChildren;
                    allTaxCounts.put(child, result);
                }

                allTaxCounts.remove(taxCount);
                //mark_for_removal.add(taxCount);


            }

        }
/*
        for(Taxon item: mark_for_removal) {
            allTaxCounts.remove(item);
        }
*/
        //remaining tax counts are leafs



    }

     /*************************************************************************//**
     *
     * @brief sets up some query options according to database parameters
     *        or command line options
     *
     *****************************************************************************/
    public void adapt_options_to_database()
    {
        //deduce hit threshold from database?
        if(this.params.getProperties().getHitsMin() < 1) {
            int sks = this.params.getProperties().getSketchlen();
            if(sks >= 6) {
                this.params.getProperties().setHitsMin((int)(sks / 3.0));
            } else if (sks >= 4) {
                this.params.getProperties().setHitsMin(2);
            } else {
                this.params.getProperties().setHitsMin(1);
            }
        }
    }


}
