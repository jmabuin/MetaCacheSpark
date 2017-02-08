package com.github.jmabuin.metacachespark;
import com.github.jmabuin.metacachespark.io.FastaInputFormat;
import com.github.jmabuin.metacachespark.io.FastqInputFormat;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import com.github.jmabuin.metacachespark.options.QueryOptions;
import com.github.jmabuin.metacachespark.spark.Fasta2Features;
import com.github.jmabuin.metacachespark.spark.Sketcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

	private MetaCacheOptions.InputFormat inputFormat;

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
						.flatMap(new Fasta2Features(sequ2taxid, infoMode)).persist(StorageLevel.MEMORY_AND_DISK_SER());//.cache();
				//databaseRDD = this.jsc.wholeTextFiles(infiles)
				//		.mapPartitionsWithIndex(new FastaSequenceReader(sequ2taxid, infoMode), true).flatMap(new Sketcher())
				//		.persist(StorageLevel.MEMORY_AND_DISK_SER());
			}
			else {

				databaseRDD = this.jsc.wholeTextFiles(infiles, this.numPartitions)
						.flatMap(new Fasta2Features(sequ2taxid, infoMode)).persist(StorageLevel.MEMORY_AND_DISK_SER());//.cache();
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


	public void query() {


		StringBuffer outfile = new StringBuffer();

		//process files / file pairs separately
		if(this.paramsQuery.isSplitOutput()) {
			//std::string outfile;
			//process each input file pair separately
			if(this.paramsQuery.getPairing() == MetaCacheOptions.pairing_mode.files && this.paramsQuery.getInfiles().length > 1) {
				for(int i = 0; i < this.paramsQuery.getInfiles().length; i += 2) {
					String f1 = this.paramsQuery.getInfiles()[i];
					String f2 = this.paramsQuery.getInfiles()[i+1];

					if(!this.paramsQuery.getOutfile().isEmpty()) {
						outfile.delete(0,outfile.length());

						int index1 = f1.lastIndexOf(File.separator);
						String fileName1 = f1.substring(index1 + 1);

						int index2 = f2.lastIndexOf(File.separator);
						String fileName2 = f1.substring(index2 + 1);

						outfile.append(this.paramsQuery.getOutfile());
						outfile.append("_");
						outfile.append(fileName1);
						outfile.append("_");
						outfile.append(fileName2);

					}

					//process_input_files(db, param, std::vector<std::string>{f1,f2}, outfile);
					String[] currentInput = {f1, f2};
					this.process_input_files(currentInput, outfile.toString());
				}
			}
			//process each input file separately
			else {
				for(String f : this.paramsQuery.getInfiles()) {
					if(!this.paramsQuery.getOutfile().isEmpty()) {
						outfile.delete(0,outfile.length());
						outfile.append(this.paramsQuery.getOutfile());
						outfile.append("_");

						int index = f.lastIndexOf(File.separator);
						String fileName = f.substring(index + 1);

						outfile.append(fileName);
						outfile.append(".txt");

					}
					//process_input_files(db, param, std::vector<std::string>{f}, outfile);
					String[] currentInput = {f};
					this.process_input_files(currentInput, outfile.toString());
				}
			}
		}
		//process all input files at once
		else {
			outfile.delete(0,outfile.length());
			outfile.append(this.paramsQuery.getOutfile());

			//process_input_files(db, param, param.infiles, param.outfile);
			this.process_input_files(this.paramsQuery.getInfiles(), outfile.toString());
		}
	}


	public void process_input_files(String[] inputfiles, String outputfile) {

		// classify_sequences(db, param, infilenames, os);
		this.classify_sequences(inputfiles, outputfile);

	}

	public void classify_sequences(String[] infilenames, String outfilename) {

		if(this.paramsQuery.getPairing() == MetaCacheOptions.pairing_mode.files) {
			classify_on_file_pairs(infilenames, outfilename);
		}
		else {
			classify_per_file(infilenames, outfilename);
		}
	}

	public void classify_on_file_pairs(String[] infilenames, String outfilename) {


		//pair up reads from two consecutive files in the list
		for(int i = 0; i < infilenames.length; i += 2) {
        	String fname1 = infilenames[i];
			String fname2 = infilenames[i+1];

/*
			classify_pairs(queue, db, param,
						*make_sequence_reader(fname1),
                           *make_sequence_reader(fname2),
						os, stats);
*/
		}

	}

	public void classify_per_file(String[] infilenames, String outfilename) {


		//pair up reads from two consecutive files in the list
		for(int i = 0; i < infilenames.length; i ++) {
			String fname = infilenames[i];



/*
			if(param.pairing == pairing_mode::sequences) {
				classify_pairs(queue, db, param, *reader, *reader, os, stats);
			} else {
				classify(queue, db, param, *reader, os, stats);
			}
*/
		}

	}


	public void classify(String filename, String outputfilename) {

		JavaPairRDD<String, String> inputData = this.loadSequencesFromFile(filename);

		JavaRDD<Location> featuresRDD;

		if(this.inputFormat == MetaCacheOptions.InputFormat.FASTA) {
			//featuresRDD = inputData.mapPartitions(new Fasta2Features());
		}
		else if (this.inputFormat == MetaCacheOptions.InputFormat.FASTA) {
			//featuresRDD = inputData.mapPartitions(new Fasta2Features());
		}



	}

	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param pathToFile The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>
	 */
	public JavaPairRDD<String, String> loadSequencesFromFile(String pathToFile) {
		JavaPairRDD<String,String> reads;

		if (pathToFile.endsWith(".fastq") || pathToFile.endsWith(".fq") || pathToFile.endsWith(".fnq")) {
			reads = this.jsc.newAPIHadoopFile(pathToFile, FastqInputFormat.class, String.class, String.class, this.jsc.hadoopConfiguration());
			this.inputFormat = MetaCacheOptions.InputFormat.FASTQ;
		}
		else {
			reads = this.jsc.newAPIHadoopFile(pathToFile, FastaInputFormat.class, String.class, String.class, this.jsc.hadoopConfiguration());
			this.inputFormat = MetaCacheOptions.InputFormat.FASTA;
		}

		return reads;

	}


}
