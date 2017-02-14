package com.github.jmabuin.metacachespark.database;
import com.github.jmabuin.metacachespark.*;
import com.github.jmabuin.metacachespark.io.FastaInputFormat;
import com.github.jmabuin.metacachespark.io.FastqInputFormat;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import com.github.jmabuin.metacachespark.options.QueryOptions;
import com.github.jmabuin.metacachespark.spark.FastaSketcher;
import com.github.jmabuin.metacachespark.spark.FastaSketcher4Query;
import com.github.jmabuin.metacachespark.spark.Sketcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import java.io.*;
import java.util.*;

/**
 * Created by chema on 1/19/17.
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
	private short nextTargetId_;
	private ArrayList<TargetProperty> targets_;
	private Dataset<Location> features_;
	private DataFrame featuresDataframe_;
	private HashMap<String,Integer> sid2gid_;
	private Taxonomy taxa_;
	private TaxonomyParam taxonomyParam;
	private int numPartitions = 1;
	private String dbfile;


	private QueryOptions paramsQuery;

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


	public Database(JavaSparkContext jsc, TaxonomyParam taxonomyParam, int numPartitions, String dbfile) {
		this.jsc = jsc;
		this.taxonomyParam = taxonomyParam;
		this.numPartitions = numPartitions;
		this.dbfile = dbfile;

		this.sqlContext = new SQLContext(this.jsc);

		this.targets_ = new ArrayList<TargetProperty>();
		this.sid2gid_ = new HashMap<String,Integer>();

	}

	public Database(JavaSparkContext jsc, String dbFile, QueryOptions params) {

		this.jsc = jsc;
		this.dbfile = dbFile;

		this.sqlContext = new SQLContext(this.jsc);

		this.paramsQuery = params;

		this.queryWindowSize_ = params.getWinlen();
		this.queryWindowStride_ = params.getWinstride();

/*
    if(param.showDBproperties) {
        print_properties(db);
        std::cout << '\n';
    }
*/


		this.loadFromFile();

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
	public Taxon taxon_of_target(Long id) {
		return taxa_.getTaxa_().get(targets_.get((int)id.longValue()).getTax());
	}

	public long taxon_id_of_target(Long id) {
		return taxa_.getTaxa_().get(targets_.get((int)id.longValue()).getTax()).getTaxonId();
	}

	public void update_lineages(TargetProperty gp)
	{
		if(gp.getTax() > 0) {
			gp.setFull_lineage(taxa_.lineage(gp.getTax()));
			gp.setRanked_lineage(taxa_.ranks((Long)gp.getTax()));
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
						.flatMap(new FastaSketcher(sequ2taxid, infoMode)).persist(StorageLevel.MEMORY_AND_DISK_SER());//.cache();
				//databaseRDD = this.jsc.wholeTextFiles(infiles)
				//		.mapPartitionsWithIndex(new FastaSequenceReader(sequ2taxid, infoMode), true).flatMap(new Sketcher())
				//		.persist(StorageLevel.MEMORY_AND_DISK_SER());
			}
			else {

				databaseRDD = this.jsc.wholeTextFiles(infiles, this.numPartitions)
						.flatMap(new FastaSketcher(sequ2taxid, infoMode)).persist(StorageLevel.MEMORY_AND_DISK_SER());//.cache();
				//databaseRDD = this.jsc.wholeTextFiles(infiles, this.numPartitions)
				//		.mapPartitionsWithIndex(new FastaSequenceReader(sequ2taxid, infoMode), true).flatMap(new Sketcher())
				//		.persist(StorageLevel.MEMORY_AND_DISK_SER());
			}


			this.featuresDataframe_ = this.sqlContext.createDataFrame(databaseRDD, Location.class);
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
			System.exit(1);
		}
		catch (Exception e) {
			LOG.error("General error accessing HDFS in rank_targets_post_process: "+e.getMessage());
			System.exit(1);
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


		try {

			//JavaSparkContext javaSparkContext = new JavaSparkContext(sparkS.sparkContext());
			//FileSystem fs = FileSystem.get(javaSparkContext.hadoopConfiguration());
			FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());

			String path = fs.getHomeDirectory().toString();

			long startTime = System.nanoTime();
			this.featuresDataframe_.write().parquet(path+"/"+this.dbfile);
			LOG.warn("Time in write parquet database is: "+ (System.nanoTime() - startTime)/10e9);
			//fs.close();

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

		this.featuresDataframe_ = this.sqlContext.read().parquet(this.dbfile).cache();

		this.featuresDataframe_.registerTempTable("parquetFeatures");


		DataFrame result = this.sqlContext.sql("select * from parquetFeatures where partitionId=0 and fileId = 0");

		LOG.warn("Number of items with partitionID=0 and gileId = 0 is: "+result.count());

	}


	public TreeMap<Location, Integer> matches(Sketch query) {
		TreeMap<Location, Integer> res = new TreeMap<Location, Integer>(new LocationComparator());
		accumulate_matches(query, res);
		return res;
	}


	public void	accumulate_matches(Sketch query, TreeMap<Location, Integer> res) {

		List<Location> obtainedLocations = null;

		// Made queries
		//for(Sketch currentLocation: query) {

			for(int i:query.getFeatures()){
				String queryString = "select * from parquetFeatures where key="+i;

				DataFrame result = this.sqlContext.sql(queryString);

				obtainedLocations = result.javaRDD().map(new Function<Row, Location>() {
					public Location call(Row row) {

						return new Location(row.getInt(0), row.getInt(1), row.getInt(2));

					}
				}).collect();
			}


			if(obtainedLocations != null) {
				for(Location obtainedLocation: obtainedLocations) {

					if(res.containsKey(obtainedLocation)) {
						res.put(obtainedLocation, res.get(obtainedLocation) +1);
					}
					else {
						res.put(obtainedLocation, 1);
					}

				}
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
		Long data[] = targets_.get((int)id).getRanked_lineage();

		long returnedData[] = new long[data.length];

		for(int i = 0; i< data.length; i++){
			returnedData[i] = data[i].longValue();
		}

		return returnedData;
	}

	public Taxon ranked_lca_of_targets(int ta, int tb) {
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

}
