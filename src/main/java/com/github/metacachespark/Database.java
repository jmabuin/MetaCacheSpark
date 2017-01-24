package com.github.metacachespark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 * Created by chema on 1/19/17.
 */
public class Database {

	private static final Log LOG = LogFactory.getLog(Database.class);

	private Random urbg_;
	private Sketcher targetSketcher_;
	private Sketcher querySketcher_;
	private long targetWindowSize_;
	private long targetWindowStride_;
	private long queryWindowSize_;
	private long queryWindowStride_;
	private long maxLocsPerFeature_;
	private short nextTargetId_;
	private ArrayList<TargetProperty> targets_;
	private Dataset<Row> features_;
	private HashMap<String,Integer> sid2gid_;
	private Taxonomy taxa_;
	private TaxonomyParam taxonomyParam;
	private int numPartitions = 1;
	private String dbfile;
	private SparkSession sparkS;

	public Database(Sketcher targetSketcher_, Sketcher querySketcher_, long targetWindowSize_, long targetWindowStride_,
					long queryWindowSize_, long queryWindowStride_, long maxLocsPerFeature_, short nextTargetId_,
					ArrayList<TargetProperty> targets_, Dataset<Row> features_, HashMap<String, Integer> sid2gid_,
					Taxonomy taxa_, SparkSession sparkS, int numPartitions) {

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

		this.sparkS = sparkS;
		this.numPartitions = numPartitions;
	}

	public Database(SparkSession sparkS, TaxonomyParam taxonomyParam, int numPartitions, String dbfile) {
		this.sparkS = sparkS;
		this.taxonomyParam = taxonomyParam;
		this.numPartitions = numPartitions;
		this.dbfile = dbfile;
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

	public short getNextTargetId_() {
		return nextTargetId_;
	}

	public void setNextTargetId_(short nextTargetId_) {
		this.nextTargetId_ = nextTargetId_;
	}

	public ArrayList<TargetProperty> getTargets_() {
		return targets_;
	}

	public void setTargets_(ArrayList<TargetProperty> targets_) {
		this.targets_ = targets_;
	}

	public Dataset<Row> getFeatures_() {
		return features_;
	}

	public void setFeatures_(Dataset<Row> features_) {
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

	public SparkSession getSparkS() {
		return sparkS;
	}

	public void setSparkS(SparkSession sparkS) {
		this.sparkS = sparkS;
	}

	/*
		const taxon&
		taxon_of_target(target_id id) const noexcept {
			return taxa_[targets_[id].taxonId];
		}
		 */
	public Taxon taxon_of_target(Long id) {
		return taxa_.getTaxa_().get((int)targets_.get((int)id.longValue()).getTax());
	}

	public void update_lineages(TargetProperty gp)
	{
		if(gp.getTax() > 0) {
			gp.setFull_lineage(taxa_.lineage(gp.getTax()));
			gp.setRanked_lineage(taxa_.ranks(gp.getTax()));
		}
	}

	public void apply_taxonomy(Taxonomy tax) {
		this.taxa_ = tax;
		for(TargetProperty g : this.targets_)
			update_lineages(g);
	}

	public long taxon_count() {
		return this.taxa_.taxon_count();
	}

	public long target_count() {
		return this.targets_.size();
	}

	public void buildDatabase(SparkSession sparkS, String infiles, HashMap<String, Long> sequ2taxid) {


	}

	public void buildDatabase(String infiles, HashMap<String, Long> sequ2taxid, Build.build_info infoMode) {

		JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());

		JavaPairRDD<String,String> inputData = javaSparkContext.wholeTextFiles(infiles, this.numPartitions);
/*
		JavaPairRDD<Integer, ArrayList<Location>> databaseDataRDD = inputData.mapPartitionsWithIndex(new FastaSequenceReader(), true)
				.mapToPair(new PairFunction<Tuple2<Integer,ArrayList<Location>>, Integer, ArrayList<Location>>() {

					public Tuple2<Integer, ArrayList<Location>> call(Tuple2<Integer, ArrayList<Location>> arg0) {
						//Iterable<Location> iterable = arg0._2();
						//	return new Tuple2<Integer, Iterable<Location>>(arg0._1(), iterable);
						return arg0;
						}
					}
				).reduceByKey(new Function2<ArrayList<Location>, ArrayList<Location>, ArrayList<Location>>() {
					public ArrayList<Location> call(ArrayList<Location> locations, ArrayList<Location> locations2) throws Exception {
						locations.addAll(locations2);
						return locations;
					}
				});

		JavaRDD<DatabaseRow> databaseRDD = databaseDataRDD.map(new Function<Tuple2<Integer, ArrayList<Location>>, DatabaseRow>() {
			public DatabaseRow call(Tuple2<Integer, ArrayList<Location>> integerArrayListTuple2) throws Exception {
				return new DatabaseRow(integerArrayListTuple2._1(), integerArrayListTuple2._2());
			}
		});

		Dataset<Row> datasetDB = sparkS.createDataFrame(databaseRDD, DatabaseRow.class);
*/
		JavaRDD<Location> databaseDataRDD = inputData.mapPartitionsWithIndex(new FastaSequenceReader(sequ2taxid, infoMode), true);

		Dataset<Row> datasetDB = sparkS.createDataFrame(databaseDataRDD, Location.class);

		this.features_ = datasetDB;

		//datasetDB.write().parquet(dbfile);

	}



	// These infilenames are taxonomies???
	HashMap<String, Long> make_sequence_to_taxon_id_map(ArrayList<String> mappingFilenames,ArrayList<String> infilenames) {
	//HashMap<String, Long> make_sequence_to_taxon_id_map(ArrayList<String> mappingFilenames,String infilenames)	{
		//gather all taxonomic mapping files that can be found in any
		//of the input directories


		HashMap<String, Long> map = new HashMap<String, Long>();

		for(String newFile: mappingFilenames) {
			map = read_sequence_to_taxon_id_mapping(newFile, map);
		}

		return map;

	}

	public HashMap<String, Long> read_sequence_to_taxon_id_mapping(String mappingFile, HashMap<String, Long> map){


		try {
			JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());
			FileSystem fs = FileSystem.get(javaSparkContext.hadoopConfiguration());
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

			header = header.replaceFirst("# ", "");

			String headerSplits[] = header.split("\t");

			for(String headerField: headerSplits) {
				if(headerField == "taxid") {
					taxcol = col;
				}
				else if (header == "accession.version" || header == "assembly_accession") {
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


			}




		}
		catch (IOException e) {
			LOG.error("I/O Error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}
		catch (Exception e) {
			LOG.error("General error accessing HDFS: "+e.getMessage());
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

			JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());
			FileSystem fs = FileSystem.get(javaSparkContext.hadoopConfiguration());
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
		}

		catch (IOException e) {
			LOG.error("I/O Error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}
		catch (Exception e) {
			LOG.error("General error accessing HDFS: "+e.getMessage());
			System.exit(1);
		}

	}


	public void remove_ambiguous_features(Taxonomy.Rank r, int maxambig) {
		if(this.taxa_.empty()) {
			LOG.error("No taxonomy available!");
			System.exit(1);
		}

		if(maxambig == 0) maxambig = 1;
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
		this.features_.write().parquet(this.dbfile);
	}

}
