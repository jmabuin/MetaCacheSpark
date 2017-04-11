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
import com.github.jmabuin.metacachespark.options.BuildOptions;
import com.github.jmabuin.metacachespark.options.QueryOptions;
import com.github.jmabuin.metacachespark.spark.*;
import com.google.common.collect.HashMultimap;
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
	private ArrayList<TargetProperty> targets_;
	private Dataset<Location> features_;
	private DataFrame featuresDataframe_;
	private DataFrame targetPropertiesDataframe_;
	private JavaRDD<TargetProperty> targetPropertiesJavaRDD;
	private HashMap<String,Integer> sid2gid_;
	private Taxonomy taxa_;
	private TaxonomyParam taxonomyParam;
	private int numPartitions = 1;
	private String dbfile;

	private JavaRDD<Sequence> inputSequences;

	private QueryOptions paramsQuery;
	private BuildOptions paramsBuild;
	private IndexedRDD<Integer, LocationBasic> indexedRDD;
	private JavaPairRDD<Integer, LocationBasic> locationJavaPairRDD;
	private JavaPairRDD<Integer, List<LocationBasic>> locationJavaPairListRDD;
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
					ArrayList<TargetProperty> targets_, Dataset<Location> features_, HashMap<String, Integer> sid2gid_,
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
		this.sid2gid_ = sid2gid_;
		this.taxa_ = taxa_;

		this.jsc = jsc;
		this.numPartitions = numPartitions;
	}


	public Database(JavaSparkContext jsc, TaxonomyParam taxonomyParam, BuildOptions paramsBuild, int numPartitions, String dbfile) {
		this.jsc = jsc;
		this.taxonomyParam = taxonomyParam;
		this.numPartitions = numPartitions;
		this.dbfile = dbfile;

		this.sqlContext = new SQLContext(this.jsc);

		this.targets_ = new ArrayList<TargetProperty>();
		this.sid2gid_ = new HashMap<String,Integer>();

		this.paramsBuild = paramsBuild;

	}

	/**
	 * @brief Builder to read database from HDFS
	 * @param jsc
	 * @param dbFile
	 * @param params
	 */
	public Database(JavaSparkContext jsc, String dbFile, QueryOptions params) {

		this.jsc = jsc;
		this.dbfile = dbFile;

		this.sqlContext = new SQLContext(this.jsc);

		this.paramsQuery = params;

		this.queryWindowSize_ = params.getWinlen();
		this.queryWindowStride_ = params.getWinstride();

		if(this.queryWindowStride_ == 0) {
			this.queryWindowStride_ = MCSConfiguration.winstride;
		}

		if(this.targetWindowStride_ == 0) {
			this.targetWindowStride_ = MCSConfiguration.winstride;
		}

		this.numPartitions = this.paramsQuery.getNumPartitions();


/*
    if(param.showDBproperties) {
        print_properties(db);
        std::cout << '\n';
    }
*/


		this.loadFromFile();
		this.readTaxonomy();
		this.readTargets();
		this.readSid2gid();

		this.nextTargetId_ = this.sid2gid_.size();
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

	public ArrayList<TargetProperty> getTargets_() {
		return targets_;
	}

	public void setTargets_(ArrayList<TargetProperty> targets_) {
		this.targets_ = targets_;
	}

	public Dataset<Location> getFeatures_() {
		return features_;
	}

	public void setFeatures_(Dataset<Location> features_) {
		this.features_ = features_;
	}

	public HashMap<String, Integer> getSid2gid_() {
		return sid2gid_;
	}

	public void setSid2gid_(HashMap<String, Integer> sid2gid_) {
		this.sid2gid_ = sid2gid_;
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

/*
	public void buildDatabase(String infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {
		try {
			LOG.warn("Starting to build database from " + infiles + " ...");
			//FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			long initTime = System.nanoTime();
			long endTime;

			//SQLContext sqlContext = new SQLContext(this.jsc);

			JavaPairRDD<String,String> inputData;
			JavaRDD<Location> databaseRDD;


			if(this.numPartitions == 1) {
				databaseRDD = this.jsc.wholeTextFiles(infiles)
						.flatMap(new FastaSketcher(sequ2taxid, infoMode));//.persist(StorageLevel.MEMORY_AND_DISK_SER());//.cache();

				this.featuresDataframe_ = this.sqlContext.createDataFrame(databaseRDD, Location.class).persist(StorageLevel.MEMORY_AND_DISK_SER());;
			}
			else {

				databaseRDD = this.jsc.wholeTextFiles(infiles, this.numPartitions)
						.flatMap(new FastaSketcher(sequ2taxid, infoMode));//.persist(StorageLevel.MEMORY_AND_DISK_SER());//.cache();

				this.featuresDataframe_ = this.sqlContext.createDataFrame(databaseRDD, Location.class).persist(StorageLevel.MEMORY_AND_DISK_SER());
			}


			//this.featuresDataframe_ = this.sqlContext.createDataFrame(databaseRDD, Location.class);
			//Encoder<Location> encoder = Encoders.bean(Location.class);
			//Dataset<Location> ds = new Dataset<Location>(sqlContext, this.featuresDataframe_.logicalPlan(), encoder);

			//this.features_ = ds;
			endTime = System.nanoTime();
			LOG.warn("Time in create database: "+ ((endTime - initTime)/1e9));
			LOG.warn("Database created ...");
			LOG.warn("Number of items into database: " + String.valueOf(this.featuresDataframe_.count()));



		} catch (Exception e) {
			LOG.error("[JMAbuin] ERROR! "+e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
*/
	public void buildDatabase2(String infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {
		try {
			LOG.warn("Starting to build database from " + infiles + " ...");
			//FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			long initTime = System.nanoTime();
			long endTime;


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


			JavaRDD<String> sequenceIdsRDD = this.inputSequences.map(new Sequence2SequenceId());

			List<String> data = sequenceIdsRDD.collect();

			sequenceIdsRDD.unpersist();
			sequenceIdsRDD = null;

			//HashMap<String, Integer> sequencesIndexes = new HashMap<String,Integer>();

			int i = 0;

			for(String value: data) {
				this.sid2gid_.put(value, i);
				i++;
			}

			this.nextTargetId_ = this.sid2gid_.size();

			//this.locationJavaPairRDD = this.inputSequences.flatMapToPair((new Sketcher2Pair(this.sid2gid_)))

			/*this.locationJavaRDD = this.inputSequences
					.flatMap(new Sketcher(this.sid2gid_));

			this.featuresDataframe_ = this.sqlContext.createDataFrame(this.locationJavaRDD, Location.class);
*/

			if(paramsBuild.isBuildModeHashMap()) {
				LOG.warn("Building database with isBuildModeHashMap");
				this.locationJavaHashRDD = this.inputSequences
						.mapPartitionsWithIndex(new Sketcher2HashMap(this.sid2gid_), true);
			}
			else if(paramsBuild.isBuildModeHashMultiMapG()) {
				LOG.warn("Building database with isBuildModeHashMultiMapG");
				this.locationJavaHashMMRDD = this.inputSequences
						.mapPartitionsWithIndex(new Sketcher2HashMultiMap(this.sid2gid_), true);
			}
			else if(paramsBuild.isBuildModeHashMultiMapMC()) {
				LOG.warn("Building database with isBuildModeHashMultiMapMC");
				//this.locationJavaPairListRDD = this.inputSequences
				//		.mapPartitionsToPair(new Sketcher2PairPartitions(this.sid2gid_), true);
				this.locationJavaRDDHashMultiMapNative = this.inputSequences
						.mapPartitionsWithIndex(new Sketcher2HashMultiMapNative(this.sid2gid_), true);
			}
			else if(paramsBuild.isBuildModeParquetDataframe()) {
				LOG.warn("Building database with isBuildModeParquetDataframe");
				this.locationJavaRDD = this.inputSequences
						.flatMap(new Sketcher(this.sid2gid_));

				this.featuresDataframe_ = this.sqlContext.createDataFrame(this.locationJavaRDD, Location.class);

			}
			else if(paramsBuild.isBuildCombineByKey()) {
				LOG.warn("Building database with isBuildCombineByKey");
				this.locationJavaPairListRDD = this.inputSequences
						.flatMapToPair(new Sketcher2Pair(this.sid2gid_))
						.partitionBy(new MyCustomPartitioner(this.paramsBuild.getNumPartitions()))
						.combineByKey(new LocationCombiner(),
								new LocationMergeValues(),
								new LocationMergeCombiners());

			}

			this.targetPropertiesJavaRDD = this.inputSequences
					.map(new Sequence2TargetProperty(this.sid2gid_));

			//this.writeTargets();
			//this.features_ = ds;
			endTime = System.nanoTime();
			LOG.warn("Time in create database: "+ ((endTime - initTime)/1e9));
			LOG.warn("Database created ...");
			//LOG.warn("Number of items into database: " + String.valueOf(this.featuresDataframe_.count()));


		} catch (Exception e) {
			LOG.error("[JMAbuin] ERROR! "+e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void buildDatabaseMulti2(ArrayList<String> infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {
		try {

			//FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			long initTime = System.nanoTime();
			long endTime;

			JavaPairRDD<String,String> inputData = null;

			this.inputSequences = null;
			//JavaPairRDD<TargetProperty, ArrayList<Location>> databaseRDD = null;


			for(String currentDir: infiles) {
				LOG.warn("Starting to build database from " + currentDir + " ...");



				if(inputData == null) {

					if(this.numPartitions != 1) {
						inputData = this.jsc.wholeTextFiles(currentDir, this.numPartitions);
					}
					else {
						inputData = this.jsc.wholeTextFiles(currentDir);
					}

				}
				else {

					if(this.numPartitions != 1) {
						inputData = this.jsc.wholeTextFiles(currentDir, this.numPartitions)
							.union(inputData);
					}
					else {
						inputData = this.jsc.wholeTextFiles(currentDir)
							.union(inputData);
					}

				}
			}

			//inputData = inputData.coalesce(this.numPartitions);


			if(this.numPartitions != 1) {
				this.inputSequences = inputData.flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
						.repartition(this.numPartitions)
						.persist(StorageLevel.MEMORY_AND_DISK_SER());
			}
			else {
				this.inputSequences = inputData.flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
						.persist(StorageLevel.MEMORY_AND_DISK_SER());
			}

			List<String> data = this.inputSequences.map(new Sequence2SequenceId()).collect();


			//inputData.unpersist();

			//HashMap<String, Integer> sequencesIndexes = new HashMap<String,Integer>();

			int i = 0;

			for(String value: data) {
				this.sid2gid_.put(value, i);
				i++;
			}

			this.nextTargetId_ = this.sid2gid_.size();

			this.locationJavaRDD = this.inputSequences
					.flatMap(new Sketcher(this.sid2gid_));

			this.featuresDataframe_ = this.sqlContext.createDataFrame(this.locationJavaRDD, Location.class);




			this.targetPropertiesJavaRDD = this.inputSequences
					.map(new Sequence2TargetProperty(this.sid2gid_));

			//this.features_ = ds;
			endTime = System.nanoTime();
			LOG.warn("Time in create database: "+ ((endTime - initTime)/1e9));
			LOG.warn("Database created ...");
			LOG.warn("Number of items into database: " + String.valueOf(this.featuresDataframe_.count()));



		} catch (Exception e) {
			LOG.error("[JMAbuin] ERROR! "+e.getMessage());
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


			if(this.paramsBuild.isMyWholeTextFiles()) {

				//LOG.warn("Starting to build database from with MyWholeTextFiles");


				JavaPairRDD<String, String> tmpInput = null;

				for (String currentDir : infiles) {
					//LOG.warn("Starting to build database from " + currentDir + " ...");

					if (tmpInput == null) {
						tmpInput = this.jsc.wholeTextFiles(currentDir);
					} else {
						tmpInput = this.jsc.wholeTextFiles(currentDir)
								.union(tmpInput);
					}
				}

				inputData = tmpInput
						.keys()
						.repartition(this.paramsBuild.getNumPartitions())
						.mapPartitionsToPair(new MyWholeTextFiles(), true)
						.persist(StorageLevel.DISK_ONLY());

				//LOG.warn("Number of partitions for input files:" + inputData.count());

				this.inputSequences = inputData
						.flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
						.persist(StorageLevel.DISK_ONLY());
			}
			else {

				for (String currentDir : infiles) {
					//LOG.warn("Starting to build database from " + currentDir + " ...");

					if (inputData == null) {
						inputData = this.jsc.wholeTextFiles(currentDir);
					} else {
						inputData = this.jsc.wholeTextFiles(currentDir)
								.union(inputData);
					}
				}

				if(this.numPartitions != 1) {
					this.inputSequences = inputData.flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
							.repartition(this.numPartitions)
							//.persist(StorageLevel.MEMORY_AND_DISK_SER());
							.persist(StorageLevel.DISK_ONLY());
				}
				else {
					this.inputSequences = inputData.flatMap(new FastaSequenceReader(sequ2taxid, infoMode))
							//.persist(StorageLevel.MEMORY_AND_DISK_SER());
							.persist(StorageLevel.DISK_ONLY());
				}


			}
			//inputData = inputData.coalesce(this.numPartitions);



			List<String> data = this.inputSequences.map(new Sequence2SequenceId()).collect();


			//inputData.unpersist();

			//HashMap<String, Integer> sequencesIndexes = new HashMap<String,Integer>();

			int i = 0;

			for(String value: data) {
				this.sid2gid_.put(value, i);
				i++;
			}

			this.nextTargetId_ = this.sid2gid_.size();


			if(paramsBuild.isBuildModeHashMap()) {
				LOG.warn("Building database with isBuildModeHashMap");
				this.locationJavaHashRDD = this.inputSequences
						.mapPartitionsWithIndex(new Sketcher2HashMap(this.sid2gid_), true);
			}
			else if(paramsBuild.isBuildModeHashMultiMapG()) {
				LOG.warn("Building database with isBuildModeHashMultiMapG");
				this.locationJavaHashMMRDD = this.inputSequences
						.mapPartitionsWithIndex(new Sketcher2HashMultiMap(this.sid2gid_), true);
			}
			else if(paramsBuild.isBuildModeHashMultiMapMC()) {
				LOG.warn("Building database with isBuildModeHashMultiMapMC");
				//this.locationJavaPairListRDD = this.inputSequences
				//		.mapPartitionsToPair(new Sketcher2PairPartitions(this.sid2gid_), true);
				this.locationJavaRDDHashMultiMapNative = this.inputSequences
						//.mapPartitionsWithIndex(new Sketcher2HashMultiMapNative(this.sid2gid_), true);
						.flatMapToPair(new Sketcher2Pair(this.sid2gid_))
						//.partitionBy(new MyCustomPartitioner(this.paramsBuild.getNumPartitions()))
						.mapPartitionsWithIndex(new Pair2HashMapNative(), true);
			}
			else if(paramsBuild.isBuildModeHashMultiMapMCBuffered()) {
				LOG.warn("Building database with isBuildModeHashMultiMapMCBuffered");
				//this.locationJavaPairListRDD = this.inputSequences
				//		.mapPartitionsToPair(new Sketcher2PairPartitions(this.sid2gid_), true);
				this.locationJavaRDDHashMultiMapNative = this.inputSequences
						//.mapPartitionsWithIndex(new Sketcher2HashMultiMapNative(this.sid2gid_), true);
						.flatMapToPair(new Sketcher2Pair(this.sid2gid_))
						.partitionBy(new MyCustomPartitioner(this.paramsBuild.getNumPartitions()))
						.mapPartitionsWithIndex(new Pair2HashMapNative(), true);
			}
			else if(paramsBuild.isBuildModeParquetDataframe()) {
				LOG.warn("Building database with isBuildModeParquetDataframe");
				this.locationJavaRDD = this.inputSequences
						.flatMap(new Sketcher(this.sid2gid_));

				this.featuresDataframe_ = this.sqlContext.createDataFrame(this.locationJavaRDD, Location.class);

			}
			else if(paramsBuild.isBuildCombineByKey()) {
				LOG.warn("Building database with isBuildCombineByKey");
				this.locationJavaPairListRDD = this.inputSequences
						.flatMapToPair(new Sketcher2Pair(this.sid2gid_))
						.partitionBy(new MyCustomPartitioner(this.paramsBuild.getNumPartitions()))
						.combineByKey(new LocationCombiner(),
								new LocationMergeValues(),
								new LocationMergeCombiners());

			}

			this.targetPropertiesJavaRDD = this.inputSequences
					.map(new Sequence2TargetProperty(this.sid2gid_));


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

		this.targetPropertiesJavaRDD.saveAsObjectFile(targetsDestination);
		//this.targets_ = new ArrayList<TargetProperty>(inputSequences.map(new Sequence2TargetProperty()).collect());


	}

	public void readTargets() {

		this.targetPropertiesJavaRDD = this.jsc.objectFile(this.dbfile+"_targets");

		this.targets_ = new ArrayList<TargetProperty>(this.targetPropertiesJavaRDD.collect());
	}


	public void writeSid2gid() {

		String sig2sidDestination = this.dbfile+"_sig2sid";

		// Try to open the filesystem (HDFS) and sequence file
		try {


			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
			FSDataOutputStream outputStream = fs.create(new Path(sig2sidDestination));

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

			// Write data
			bw.write(String.valueOf(this.sid2gid_.size()));
			bw.newLine();

			StringBuffer currentLine = new StringBuffer();

			for(Map.Entry<String, Integer> currentEntry: this.sid2gid_.entrySet()) {

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

			if(this.sid2gid_ == null) {
				this.sid2gid_ = new HashMap<String, Integer>();
			}

			// read data
			long numberItems = Long.parseLong(br.readLine());

			String currentLine;

			while((currentLine = br.readLine()) != null) {

				String parts[] = currentLine.split(":");

				this.sid2gid_.put(parts[0], Integer.parseInt(parts[1]));

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
			map = read_sequence_to_taxon_id_mapping(newFile, map);
		}

		return map;

	}

	public HashMap<String, Long> read_sequence_to_taxon_id_mapping(String mappingFile, HashMap<String, Long> map){


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


		return map;


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
		//String it = sid2gid_..find(sid);

		if(sid2gid_.containsKey(sid)) {
			return sid2gid_.get(sid);
		}
		else {
			return (int)nextTargetId_;
		}

		//return (it != sid2gid_.end()) ? it->second : nextTargetId_;
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

			if(paramsBuild.isBuildModeHashMap()) {
				//this.locationJavaHashRDD.saveAsObjectFile(path+"/"+this.dbfile);

				List<String> outputs = this.locationJavaHashRDD
						.mapPartitionsWithIndex(new WriteHashMap(path+"/"+this.dbfile), true)
						.collect();

				for(String output: outputs) {
					LOG.warn("Writed file: "+output);
				}

			}
			else if(paramsBuild.isBuildModeHashMultiMapG()) {
				this.locationJavaHashMMRDD.saveAsObjectFile(path+"/"+this.dbfile);
			}
			else if(paramsBuild.isBuildModeHashMultiMapMC() || paramsBuild.isBuildModeHashMultiMapMCBuffered()) {
				//this.locationJavaPairListRDD.saveAsObjectFile(path+"/"+this.dbfile);
				//this.locationJavaRDDHashMultiMapNative.saveAsObjectFile(path+"/"+this.dbfile);

				List<String> outputs = this.locationJavaRDDHashMultiMapNative.mapPartitionsWithIndex(
						new WriteHashMapNative(path+"/"+this.dbfile), true).collect();

				for(String output: outputs) {
					LOG.warn("Writed file: "+output);
				}

			}
			else if(paramsBuild.isBuildModeParquetDataframe()) {
				this.featuresDataframe_.write().parquet(path+"/"+this.dbfile);
				//this.featuresDataframe_.write().partitionBy("key").parquet(path+"/"+this.dbfile);
			}
			else if(paramsBuild.isBuildCombineByKey()) {
				this.locationJavaPairListRDD.saveAsObjectFile(path+"/"+this.dbfile);

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
		if(paramsQuery.isBuildModeHashMap()) {
			LOG.warn("Loading database with isBuildModeHashMap");
			//this.locationJavaHashRDD = this.jsc.objectFile(this.dbfile).map(new Obj2HashMap());

			JavaRDD<HashMap<Integer, List<LocationBasic>>> tmpRDD;

			if(this.paramsQuery.getNumPartitions() != 1) {
				tmpRDD = this.jsc.wholeTextFiles(this.dbfile, this.paramsQuery.getNumPartitions())
						.keys()
						.mapPartitions(new ReadHashMapPartitions(), true)
						//.map(new ReadHashMap())
						.persist(StorageLevel.DISK_ONLY());
			}
			else {
				tmpRDD = this.jsc.wholeTextFiles(this.dbfile)
						.keys()
						.mapPartitions(new ReadHashMapPartitions(), true)
						//.map(new ReadHashMap())
						.persist(StorageLevel.DISK_ONLY());
			}

			LOG.warn("The number of persisted HashMaps is: " + tmpRDD.count());

			this.locationJavaHashRDD = tmpRDD
					//.map(new HashMapIdentity()) // HashMapIdentity implements Function<HashMap<Integer, List<LocationBasic>>, HashMap<Integer, List<LocationBasic>>>
					.persist(StorageLevel.MEMORY_AND_DISK_SER());

			//this.locationJavaHashRDD.persist(StorageLevel.MEMORY_AND_DISK());


		}
		else if(paramsQuery.isBuildModeHashMultiMapG()) {
			LOG.warn("Loading database with isBuildModeHashMultiMapG");
			this.locationJavaHashMMRDD = this.jsc.objectFile(this.dbfile);

			this.locationJavaHashMMRDD.persist(StorageLevel.MEMORY_AND_DISK());

			LOG.warn("The number of persisted HashMultimaps is: " + this.locationJavaHashMMRDD.count());

			/*
			LOG.warn("Loading database with isBuildModeHashMultiMapMC-G");
			//this.locationJavaPairListRDD = JavaPairRDD.fromJavaRDD(this.jsc.objectFile(this.dbfile));

			List<String> filesNames = FilesysUtility.files_in_directory(this.dbfile, 0);

			for(String newFile : filesNames) {
				LOG.warn("New file added: "+newFile);
			}

			JavaRDD<String> filesNamesRDD;

			if(this.paramsQuery.getNumPartitions() != 1) {
				filesNamesRDD = this.jsc.parallelize(filesNames, this.paramsQuery.getNumPartitions());
			}
			else {
				filesNamesRDD = this.jsc.parallelize(filesNames);
			}



			this.locationJavaRDDHashMultiMapNative = filesNamesRDD
					.mapPartitionsWithIndex(new ReadHashMapNative(), true)
					.persist(StorageLevel.MEMORY_AND_DISK());


			LOG.warn("The number of paired persisted entries is: " + this.locationJavaRDDHashMultiMapNative.count());
			*/
		}
		else if(paramsQuery.isBuildModeHashMultiMapMC() || paramsQuery.isBuildModeHashMultiMapMCBuffered()) {
			LOG.warn("Loading database with isBuildModeHashMultiMapMC");
			//this.locationJavaPairListRDD = JavaPairRDD.fromJavaRDD(this.jsc.objectFile(this.dbfile));

			List<String> filesNames = FilesysUtility.files_in_directory(this.dbfile, 0);

			for(String newFile : filesNames) {
				LOG.warn("New file added: "+newFile);
			}

			JavaRDD<String> filesNamesRDD;

			if(this.paramsQuery.getNumPartitions() != 1) {
				filesNamesRDD = this.jsc.parallelize(filesNames, this.paramsQuery.getNumPartitions());
			}
			else {
				filesNamesRDD = this.jsc.parallelize(filesNames);
			}



			this.locationJavaRDDHashMultiMapNative = filesNamesRDD
					.mapPartitionsWithIndex(new ReadHashMapNative(), true)
					.persist(StorageLevel.MEMORY_AND_DISK());


			LOG.warn("The number of paired persisted entries is: " + this.locationJavaRDDHashMultiMapNative.count());
		}
		else if(paramsQuery.isBuildModeParquetDataframe()) {
			LOG.warn("Loading database with isBuildModeParquetDataframe");
			DataFrame dataFrame = this.sqlContext.read().parquet(this.dbfile);

			this.locationJavaRDD = dataFrame.javaRDD()
					.map(new Row2Location());

			this.featuresDataframe_ = this.sqlContext.createDataFrame(this.locationJavaRDD, Location.class)
					.persist(StorageLevel.MEMORY_AND_DISK());

			this.featuresDataframe_.registerTempTable("MetaCacheSpark");
			//this.sqlContext.cacheTable("MetaCacheSpark");

			LOG.warn("The number of paired persisted entries is: " + this.featuresDataframe_.count());

		}
		else if(paramsQuery.isBuildCombineByKey()) {
			LOG.warn("Loading database with isBuildCombineByKey");
			this.locationJavaPairListRDD = JavaPairRDD.fromJavaRDD(this.jsc.objectFile(this.dbfile));

			this.locationJavaPairListRDD.persist(StorageLevel.MEMORY_AND_DISK());

			LOG.warn("The number of paired persisted entries is: " + this.locationJavaPairListRDD.count());
		}

	}

	public List<HashMap<LocationBasic, Integer>> matches_buffer( List<SequenceData> inputData, long init, int size, long total, long readed) {

		// Get results for this buffer
		List<Tuple2<Long,List<int[]>>> results = this.locationJavaRDDHashMultiMapNative
				.mapPartitionsToPair(new PartialQuery(inputData, init, size, total, readed))
				.reduceByKey(new QueryReducer())
				.sortByKey()
				.collect();

		List<HashMap<LocationBasic, Integer>> returnValues = new ArrayList<HashMap<LocationBasic, Integer>>();

		//LOG.warn("Starting hits counter ");

		for(Tuple2<Long,List<int[]>> current : results) {

			HashMap<LocationBasic, Integer> myHashMap = new HashMap<LocationBasic, Integer>();

			for(int[] currentLocation: current._2) {

				for(int i = 0; i< currentLocation.length; i+=2) {

					LocationBasic loc = new LocationBasic(currentLocation[i], currentLocation[i+1]);

					if(myHashMap.containsKey(loc)) {
						myHashMap.put(loc,myHashMap.get(loc)+1);
					}
					else {
						myHashMap.put(loc,1);
					}

				}

			}

			returnValues.add(myHashMap);

		}

		//LOG.warn("Ending hits counter ");


		return returnValues;

	}


	public List<TreeMap<LocationBasic, Integer>> matches_buffer_treemap( List<SequenceData> inputData, long init, int size, long total, long readed) {

		// Get results for this buffer
		List<Tuple2<Long,List<int[]>>> results = this.locationJavaRDDHashMultiMapNative
				.mapPartitionsToPair(new PartialQuery(inputData, init, size, total, readed))
				.reduceByKey(new QueryReducer())
				.sortByKey()
				.collect();

		List<TreeMap<LocationBasic, Integer>> returnValues = new ArrayList<TreeMap<LocationBasic, Integer>>();

		//LOG.warn("Starting hits counter ");

		for(Tuple2<Long,List<int[]>> current : results) {

			TreeMap<LocationBasic, Integer> myHashMap = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());

			for(int[] currentLocation: current._2) {

				for(int i = 0; i< currentLocation.length; i+=2) {

					LocationBasic loc = new LocationBasic(currentLocation[i], currentLocation[i+1]);

					if(myHashMap.containsKey(loc)) {
						myHashMap.put(loc,myHashMap.get(loc)+1);
					}
					else {
						myHashMap.put(loc,1);
					}

				}

			}

			returnValues.add(myHashMap);

		}

		//LOG.warn("Ending hits counter ");


		return returnValues;

	}


	public TreeMap<LocationBasic, Integer> matches(Sketch query) {

		if(this.paramsQuery.isBuildModeHashMap()) {
			return this.accumulate_matches_hashmapmode(query);
		}
		else if(paramsQuery.isBuildModeHashMultiMapG()) {
			return null;
		}
		else if(paramsQuery.isBuildModeHashMultiMapMC()) {
			return this.accumulate_matches_hashmapnativemode(query);
		}
		else if(this.paramsQuery.isBuildModeParquetDataframe()) {
			return this.accumulate_matches_filter(query);
		}
		else if(this.paramsQuery.isBuildCombineByKey()) {
			return this.accumulate_matches_combinemode(query);
		}

		return null;

	}


	public TreeMap<LocationBasic, Integer> accumulate_matches_filter(Sketch query) {

		TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());

		StringBuilder queryString = new StringBuilder();

		JavaRDD<LocationBasic> obtainedLocationsRDD = null;

		List<LocationBasic> obtainedLocations = new ArrayList<LocationBasic>();

		try {


			for (int i : query.getFeatures()) {
/*
				if(queryString.toString().isEmpty()) {
					queryString.append("select * from MetaCacheSpark where key = " + i);
				}
				else {
					queryString.append(" or key = "+i);
				}
*/
				Row results[] = this.featuresDataframe_.filter(this.featuresDataframe_.col("key").equalTo(i)).collect();

				for(Row currentRow: results) {
					obtainedLocations.add(new LocationBasic(currentRow.getInt(1),currentRow.getInt(2)));
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

	// Not using it
	public void accumulate_matches_lookup(ArrayList<Sketch> query, HashMap<LocationBasic, Integer> res) {
		try {

			for(Sketch currentSketch : query) {
				for (int i : currentSketch.getFeatures()) {

					List<LocationBasic> obtainedValues = this.locationJavaPairRDD.lookup(i);

					for (LocationBasic obtainedLocation : obtainedValues) {

						//Location newLocation = new Location(i, obtainedLocation.getTargetId(), obtainedLocation.getWindowId());

						if (res.containsKey(obtainedLocation)) {
							res.put(obtainedLocation, res.get(obtainedLocation) + 1);
						} else {
							res.put(obtainedLocation, 1);
						}

					}

				}

				LOG.warn("[JMAbuin] Number of locations: "+res.size());

			}

		}
		catch(Exception e) {
			e.printStackTrace();
			LOG.error("General error in accumulate_matches: "+e.getMessage());
			System.exit(1);
		}

		//}
	}

	public TreeMap<LocationBasic, Integer> accumulate_matches_combinemode(Sketch query) {

		TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());

		try {


			//for(Sketch currentSketch : query) {
				for (int i : query.getFeatures()) {

					List<List<LocationBasic>> obtainedValues = this.locationJavaPairListRDD.lookup(i);

					for(List<LocationBasic> obtainedLocations: obtainedValues){
						for (LocationBasic obtainedLocation : obtainedLocations) {

							//Location newLocation = new Location(i, obtainedLocation.getTargetId(), obtainedLocation.getWindowId());

							if (res.containsKey(obtainedLocation)) {
								res.put(obtainedLocation, res.get(obtainedLocation) + 1);
							} else {
								res.put(obtainedLocation, 1);
							}
						}

					}

				}

				LOG.warn("[JMAbuin] Number of locations: "+res.size());

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
	public TreeMap<LocationBasic, Integer> accumulate_matches_hashmapmode(Sketch query) {

		TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());

		try {


			//for(Sketch currentSketch : query) {
			for (int i : query.getFeatures()) {

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

			LOG.warn("[JMAbuin] Number of locations: "+res.size());

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

	public TreeMap<LocationBasic, Integer> accumulate_matches_hashmapnativemode(Sketch query) {

		TreeMap<LocationBasic, Integer> res = new TreeMap<LocationBasic, Integer>(new LocationBasicComparator());

		try {

			List<LocationBasic> obtainedValues = this.locationJavaRDDHashMultiMapNative
					.mapPartitions(new SearchInHashMapNativeArray(query.getFeatures()), true)
					.collect();

			//for(List<LocationBasic> obtainedLocations: obtainedValues){
			for (LocationBasic newLocation : obtainedValues) {

				if (res.containsKey(newLocation)) {
					res.put(newLocation, res.get(newLocation) + 1);
				} else {
					res.put(newLocation, 1);
				}
			}

			//LOG.warn("[JMAbuin] Number of locations: "+res.size());

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
/*
	public HashMap<Location, Integer> accumulate_matches_hashmapnativemode_hashmap(Sketch query) {

		HashMap<Location, Integer> res = new HashMap<Location, Integer>();

		try {

			List<Location> obtainedValues = this.locationJavaRDDHashMultiMapNative
					.mapPartitions(new SearchInHashMapNativeArray(query.getFeatures()), true)
					.collect();

			//for(List<LocationBasic> obtainedLocations: obtainedValues){
			for (Location newLocation : obtainedValues) {

				if (res.containsKey(newLocation)) {
					res.put(newLocation, res.get(newLocation) + 1);
				} else {
					res.put(newLocation, 1);
				}
			}

			//LOG.warn("[JMAbuin] Number of locations: "+res.size());

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
*/
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

			obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Row2Location());

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

					obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Row2Location());

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

				obtainedLocationsRDD = this.sqlContext.sql(queryString.toString()).javaRDD().map(new Row2Location());

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

	public Long[] ranks(Taxon tax)  {
		return this.taxa_.ranks((Long)tax.getTaxonId());
	}

	public Classification ground_truth(String header) {
		//try to extract query id and find the corresponding target in database
		long tid = this.target_id_of_sequence(SequenceReader.extract_ncbi_accession_version_number(header));

		//not found yet
		if(!this.is_valid((int)tid)) {
			tid = this.target_id_of_sequence(SequenceReader.extract_ncbi_accession_number(header));
			//not found yet

		}

		long taxid = SequenceReader.extract_taxon_id(header);

		//if target known and in db
		if(this.is_valid((int)tid)) {
			//if taxid could not be determined solely from header, use the one
			//from the target in the db
			if(taxid < 1) this.taxon_id_of_target(tid);

			return new Classification (tid, taxid > 0 ? this.taxon_with_id(taxid) : null );
		}

		return new Classification (taxid > 0 ? this.taxon_with_id(taxid) : null );
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
}
