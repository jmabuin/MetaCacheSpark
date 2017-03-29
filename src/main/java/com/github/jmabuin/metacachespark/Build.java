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
package com.github.jmabuin.metacachespark;

import com.github.jmabuin.metacachespark.database.Database;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.options.BuildOptions;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Build implements Serializable {

	private static final Log LOG = LogFactory.getLog(Build.class);

	// Default options values
	private BuildOptions param;

	private Database db;
	private JavaSparkContext jsc;

	public enum build_info {
		silent, moderate, verbose
	};


	public Build(BuildOptions param, JavaSparkContext jsc) {

		this.param = param;

		this.jsc = jsc;

		this.db = new Database(this.jsc, this.param.getTaxonomyParam(), this.param, this.param.getNumPartitions(), this.param.getDbfile());

		//configure sketching scheme
		/*
		auto sketcher = database::sketcher{};
		sketcher.kmer_size(param.kmerlen);
		sketcher.sketch_size(param.sketchlen);

		auto db = database{sketcher};
		db.target_window_size(param.winlen);
		db.target_window_stride(param.winstride);

		add_to_database(db, param); // next function
		*/

	}


	public void buildDatabase() {

		this.add_to_database(this.db);

	}

	public void load_taxonomy_into_database(Database db) {
		db.apply_taxonomy( make_taxonomic_hierarchy(this.param.getTaxonomyParam().getNodesFile(),
				this.param.getTaxonomyParam().getNamesFile(),
				this.param.getTaxonomyParam().getMergeFile()));


		LOG.info("Taxonomy applied to database.");
	}


	public Taxonomy make_taxonomic_hierarchy(String taxNodesFile, String taxNamesFile, String mergeTaxFile ) {

		//using taxon_id = taxonomy::taxon_id;

		//read scientific taxon names
		//failure to do so will not be fatal
		HashMap<Long, String> taxonNames = new HashMap<Long,String>();
		BufferedReader br;
		//JavaSparkContext javaSparkContext = new JavaSparkContext(this.sparkS.sparkContext());

		try {
			//br = new BufferedReader(new FileReader(taxNamesFile));
			FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			FSDataInputStream inputStream = fs.open(new Path(taxNamesFile));

			br = new BufferedReader(new InputStreamReader(inputStream));

			LOG.info("Reading taxon names ... ");

			long lastId = 0;
			long taxonId = 0;

			String category;

			for(String line; (line = br.readLine()) != null; ) {
				String[] lineParts = line.split("\\|");

				String name = null;
				//String word;

				for(int i = 0; i< lineParts.length; i++) {
					String currentvalueTrim = lineParts[i].trim();

					if(i == 0) {
						taxonId = Long.parseLong(currentvalueTrim);
					}
					else if((i == 1) && (taxonId != lastId)){
						name = currentvalueTrim;
					}
					else if((i == 3) && (taxonId != lastId)){
						category = currentvalueTrim;

						if(category.contains("scientific")) {

							lastId = taxonId;
							taxonNames.put(taxonId, name);

						}

					}


				}

			}

			br.close();
			inputStream.close();
			//fs.close();

			LOG.info("Done.");
		}
		catch (IOException e) {
			LOG.error("Could not read taxon names file "+ taxNamesFile+ " because of IO error; continuing with ids only.");
			e.printStackTrace();
			//System.exit(1);
		}
		catch (Exception e) {
			LOG.error("Could not read taxon names file "+ taxNamesFile+ "; continuing with ids only.");
			e.printStackTrace();
			//System.exit(1);
		}

		//read merged taxa
		HashMap<Long, Long> mergedTaxa = new HashMap<Long, Long>();
		try {
			//br = new BufferedReader(new FileReader(mergeTaxFile));

			FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			FSDataInputStream inputStream = fs.open(new Path(mergeTaxFile));

			br = new BufferedReader(new InputStreamReader(inputStream));

			LOG.info("Reading taxonomic node mergers ... ");

			long oldId = 0;
			long newId = 0;

			//String category;

			for(String line; (line = br.readLine()) != null; ) {
				String[] lineParts = line.split("\\|");


				for(int i = 0; i< lineParts.length; i++) {
					String currentvalueTrim = lineParts[i].trim();

					if((i == 0) && (!currentvalueTrim.isEmpty())) {
						oldId = Long.parseLong(currentvalueTrim);
					}
					else if((i == 1) && (!currentvalueTrim.isEmpty())){
						newId = Long.parseLong(currentvalueTrim);
					}

				}

				mergedTaxa.put(oldId, newId);

			}

			br.close();
			inputStream.close();
			//fs.close();

			LOG.info("Done.");
		}
		catch (IOException e) {
			LOG.error("Could not read taxonomic node mergers file "+ mergeTaxFile+ "");
			e.printStackTrace();
			System.exit(1);
		}

		//read taxonomic structure
		Taxonomy tax = new Taxonomy();

		try {
			//br = new BufferedReader(new FileReader(taxNodesFile));

			FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			FSDataInputStream inputStream = fs.open(new Path(taxNodesFile));

			br = new BufferedReader(new InputStreamReader(inputStream));

			LOG.info("Reading taxonomic tree ... ");

			long taxonId = 0;
			long parentId = 0;
			String rankName = "";

			for(String line; (line = br.readLine()) != null; ) {
				String[] lineParts = line.split("\\|");

				for(int i = 0; i< lineParts.length; i++) {
					String currentvalueTrim = lineParts[i].trim();
					if((i == 0) && (!currentvalueTrim.isEmpty())) {
						taxonId = Long.parseLong(currentvalueTrim);
					}
					else if((i == 1) && (!currentvalueTrim.isEmpty())){
						parentId = Long.parseLong(currentvalueTrim);
					}
					else if((i == 2)  && (!currentvalueTrim.isEmpty())) {
						rankName = currentvalueTrim;
					}

				}

				//get taxon name
				String taxonName = "";
				if(taxonNames.containsKey(taxonId)) {
					taxonName = taxonNames.get(taxonId);
				}
				else {
					taxonName = "--";
				}

				if(taxonName.isEmpty()) {
					taxonName = "<" + String.valueOf(taxonId) + ">";
				}

				//replace ids with new ids according to mergers
				//TODO this is stupid, handle mergers properly
				if(mergedTaxa.containsKey(taxonId)) {
					taxonId = mergedTaxa.get(taxonId);
				}

				if(mergedTaxa.containsKey(parentId)) {
					parentId = mergedTaxa.get(parentId);
				}

				tax.emplace(taxonId, parentId, taxonName, rankName);

			}

			br.close();
			inputStream.close();
			//fs.close();
			LOG.info(tax.taxon_count() + " taxa read.");
		}
		catch (IOException e) {
			LOG.error("Could not read taxonomic node mergers file "+ mergeTaxFile+ "");
			e.printStackTrace();
			System.exit(1);
		}

		//make sure every taxon has a rank designation
		tax.rank_all_unranked();
		LOG.info("End of rank_all_unranked");
		return tax;
	}


	public void add_to_database(Database db) {

		LOG.info("Beginning add to database");
		long startTime = System.nanoTime();

		if(this.param.getMax_locations_per_feature() > 0){
			db.setMaxLocsPerFeature_((long)this.param.getMax_locations_per_feature());
		}

		if(this.param.getMax_load_fac() > 0) {
			//db.max_load_factor(param.maxLoadFactor);
		}

		if(!this.param.getTaxonomyParam().getPath().isEmpty()) {
			this.load_taxonomy_into_database(this.db);
		}

		if(this.db.taxon_count() < 1) {
			LOG.info("The database doesn't contain a taxonomic hierarchy yet.\n" +
					"You can add one or update later via:\n" +
					"./metacache add <database> -taxonomy <directory>");
		}

		if(this.param.getRemoveAmbigFeaturesOnRank() != Taxonomy.Rank.none) {
			if(db.taxon_count() > 1) {
				LOG.info("Ambiguous features on rank " + Taxonomy.rank_name(this.param.getRemoveAmbigFeaturesOnRank())
				+ "will be removed afterwards.\n");
			}
			else {
				LOG.info("Could not determine amiguous features due to missing taxonomic information.\n");
			}
		}

		//db.printProperties();

		if(!this.param.getInfiles().isEmpty()) {
			LOG.info("\nProcessing reference sequences.");

			//long initNumTargets = db.target_count();

			ArrayList<String> inFilesTaxonIdMap = FilesysUtility.findInHDFS(this.param.getInfiles(),"assembly_summary.txt",this.jsc);

			/*for(String currentFile: inFilesTaxonIdMap) {

				System.err.println("[JMAbuin] "+currentFile);
			}*/

			this.add_targets_to_database(db, db.make_sequence_to_taxon_id_map(
					this.param.getTaxonomyParam().getMappingPreFiles(),
					inFilesTaxonIdMap),
					build_info.moderate);


			// This block has been moved to Database.buildDatabaseMulti2

			db.try_to_rank_unranked_targets();

			if(this.param.getRemoveAmbigFeaturesOnRank() != Taxonomy.Rank.none && db.taxon_count() > 1) {

				// TODO: To be implemented in Spark!!
				db.remove_ambiguous_features(this.param.getRemoveAmbigFeaturesOnRank(), this.param.getMaxTaxaPerFeature());
				//db.print_properties();
			}


			LOG.warn("Total build time before writing: " + (System.nanoTime() - startTime)/1e9);
			db.write_database();
			db.writeTaxonomy();
			db.writeTargets();
			db.writeSid2gid();

			long elapsedTime = System.nanoTime() - startTime;
			LOG.warn("Total build time: " + (double)elapsedTime/1e9);

		}

	}

	// add to database with spark
	public void add_targets_to_database(Database db, HashMap<String, Long> sequ2taxid, build_info infoMode) {

		ArrayList<String> inputDirs = FilesysUtility.directories_in_directory_hdfs(this.param.getInfiles(), this.jsc);

		LOG.warn("JMAbuin: Number of subdirs: " + inputDirs.size());


		if(inputDirs.isEmpty()) {

			db.buildDatabase2(this.param.getInfiles(), sequ2taxid, infoMode);
		}
		else {
			//db.buildDatabaseMulti2(inputDirs, sequ2taxid, infoMode);
			db.buildDatabaseMultiPartitions(inputDirs, sequ2taxid, infoMode);
		}



	}


}
