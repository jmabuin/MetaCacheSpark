package com.github.jmabuin.metacachespark;


import com.github.jmabuin.metacachespark.database.*;
import com.github.jmabuin.metacachespark.io.FastaInputFormat;
import com.github.jmabuin.metacachespark.io.FastqInputFormat;
import com.github.jmabuin.metacachespark.io.SequenceData;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import com.github.jmabuin.metacachespark.options.QueryOptions;
import com.github.jmabuin.metacachespark.spark.FastaSketcher4Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jabuinmo on 07.02.17.
 */
public class Query implements Serializable {

    private static final Log LOG = LogFactory.getLog(Query.class);

    // Default options values
    private QueryOptions param;

    private Database db;
    private JavaSparkContext jsc;

    private MetaCacheOptions.InputFormat inputFormat;
	private HashMap<Location, Integer> hits;

    public Query(QueryOptions param, JavaSparkContext jsc) {

        this.param = param;

        this.jsc = jsc;

        this.db = new Database(jsc, this.param.getDbfile(), this.param);

		this.hits = new HashMap<Location, Integer>();
    }

    public void query() {


        StringBuffer outfile = new StringBuffer();

        //process files / file pairs separately
        if(this.param.isSplitOutput()) {
            //std::string outfile;
            //process each input file pair separately
            if(this.param.getPairing() == MetaCacheOptions.pairing_mode.files && this.param.getInfiles().length > 1) {
                for(int i = 0; i < this.param.getInfiles().length; i += 2) {
                    String f1 = this.param.getInfiles()[i];
                    String f2 = this.param.getInfiles()[i+1];

                    if(!this.param.getOutfile().isEmpty()) {
                        outfile.delete(0,outfile.length());

                        int index1 = f1.lastIndexOf(File.separator);
                        String fileName1 = f1.substring(index1 + 1);

                        int index2 = f2.lastIndexOf(File.separator);
                        String fileName2 = f1.substring(index2 + 1);

                        outfile.append(this.param.getOutfile());
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
                for(String f : this.param.getInfiles()) {
                    if(!this.param.getOutfile().isEmpty()) {
                        outfile.delete(0,outfile.length());
                        outfile.append(this.param.getOutfile());
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
            outfile.append(this.param.getOutfile());

            //process_input_files(db, param, param.infiles, param.outfile);
            this.process_input_files(this.param.getInfiles(), outfile.toString());
        }
    }


    public void process_input_files(String[] inputfiles, String outputfile) {

        // classify_sequences(db, param, infilenames, os);
		// Open output buffer

		try {
			FileSystem fs = FileSystem.get(this.jsc.hadoopConfiguration());
			FSDataOutputStream outputStream = fs.create(new Path(outputfile), true);

			BufferedWriter d = new BufferedWriter(new OutputStreamWriter(outputStream));

			this.classify_sequences(inputfiles, d);


			d.close();
			outputStream.close();
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



    }

    public void classify_sequences(String[] infilenames, BufferedWriter d) {

    	ClassificationStatistics stats = new ClassificationStatistics();

        if(this.param.getPairing() == MetaCacheOptions.pairing_mode.files) {
            classify_on_file_pairs(infilenames, d, stats);
        }
        else {
            classify_per_file(infilenames, d, stats);
        }
    }

    public void classify_on_file_pairs(String[] infilenames, BufferedWriter d, ClassificationStatistics stats) {


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

    public void classify_per_file(String[] infilenames, BufferedWriter d, ClassificationStatistics stats) {


        //pair up reads from two consecutive files in the list
        for(int i = 0; i < infilenames.length; i ++) {
            String fname = infilenames[i];


            if(this.param.getPairing() == MetaCacheOptions.pairing_mode.sequences){

			}
			else {
            	this.classify(fname, d, stats);
			}

/*
			if(param.pairing == pairing_mode::sequences) {
				classify_pairs(queue, db, param, *reader, *reader, os, stats);
			} else {
				classify(queue, db, param, *reader, os, stats);
			}
*/
        }

    }


    public void classify_pairs(String f1, String f2, BufferedWriter d, ClassificationStatistics stats) {

    	JavaPairRDD<String, String> inputData1 = this.loadSequencesFromFile(f1);
		JavaPairRDD<String, String> inputData2 = this.loadSequencesFromFile(f2);

		JavaRDD<Sketch> featuresRDD1 = null;
		JavaRDD<Sketch> featuresRDD2 = null;

		if(this.inputFormat == MetaCacheOptions.InputFormat.FASTA) {
			featuresRDD1 = inputData1.flatMap(new FastaSketcher4Query());
		}
		else if (this.inputFormat == MetaCacheOptions.InputFormat.FASTQ) {
			//featuresRDD = inputData.mapPartitions(new FastaSketcher());
		}

		List<Sketch> locations = featuresRDD1.collect();
		List<Sketch> locations2 = featuresRDD1.collect();

		if(locations.size() != locations2.size()) {
			LOG.error("Sketches of different size!!");
			return;
		}

		for(int i = 0; i<locations.size(); i++) {
			Sketch currentSketch = locations.get(i);
			Sketch currentSketch2 = locations2.get(i);

			TreeMap<Location, Integer> matches = this.db.matches(currentSketch);

			this.db.accumulate_matches(currentSketch2, matches);

			this.process_database_answer(currentSketch.getHeader(), currentSketch.getSequence(), "", matches, d, stats);
		}


	}

    public void classify(String filename, BufferedWriter d, ClassificationStatistics stats) {

        JavaPairRDD<String, String> inputData = this.loadSequencesFromFile(filename);

        JavaRDD<Sketch> featuresRDD = null;

        if(this.inputFormat == MetaCacheOptions.InputFormat.FASTA) {
            featuresRDD = inputData.flatMap(new FastaSketcher4Query());
        }
        else if (this.inputFormat == MetaCacheOptions.InputFormat.FASTQ) {
            //featuresRDD = inputData.mapPartitions(new FastaSketcher());
        }

        List<Sketch> locations = featuresRDD.collect();

        for(Sketch currentSketch: locations) {
			TreeMap<Location, Integer> matches = this.db.matches(currentSketch);

			this.process_database_answer(currentSketch.getHeader(), currentSketch.getSequence(), "", matches, d, stats);
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

	/*****************************************************************************
	 *
	 * @brief removes hits of a taxon on a certain rank; can be very slow!
	 *
	 *****************************************************************************/
	void remove_hits_on_rank(Taxonomy.Rank rank, long taxid) {
		HashMap<Location, Integer> maskedRes = (HashMap<Location, Integer>)this.hits.clone();


		for(Location currentLocation: this.hits.keySet()) {

			long r = this.db.ranks_of_target(currentLocation.getTargetId())[rank.ordinal()];
			// Todo: This function actually works????

			if(r != taxid) {
				//maskedRes.insert(this.hits.get(currentLocation));
			}
		}

		//using std::swap;
		//swap(maskedRes,this.hits);
	}

	public void process_database_answer(String header, String query1, String query2, TreeMap<Location, Integer> hits,
										BufferedWriter d, ClassificationStatistics stats) {
			/*
     const database& db, const query_param& param,
     const std::string& header,
     const sequence& query1, const sequence& query2,
	 match_result&& hits, std::ostream& os, classification_statistics& stats)
	 */

		if(header.isEmpty()) return;

		//preparation -------------------------------
		Classification groundTruth = new Classification();

		if(this.param.isTestPrecision() ||
				(this.param.getMapViewMode() != MetaCacheOptions.map_view_mode.none && this.param.isShowGroundTruth()) ||
				(this.param.getExcludedRank() != Taxonomy.Rank.none) ) {

			groundTruth = this.db.ground_truth( header);

		}

		//clade exclusion
		if(this.param.getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
			long exclTaxid = this.db.ranks(groundTruth.tax())[this.param.getExcludedRank().ordinal()];
			remove_hits_on_rank( this.param.getExcludedRank(), exclTaxid); //Todo: Look what this function does
		}

		//classify ----------------------------------
		long numWindows = ( 2 + Math.max(query1.length() + query2.length(),this.param.getInsertSizeMax()) / this.db.getTargetWindowStride_());

		MatchesInWindow tophits = new MatchesInWindow(hits, numWindows);
		Classification cls = this.sequence_classification(tophits);

		if(param.isTestPrecision()) {
			Taxonomy.Rank lowestCorrectRank = db.lowest_common_rank( cls, groundTruth);

			stats.assign_known_correct(cls.rank(), groundTruth.rank(), lowestCorrectRank);

			//check if taxa of assigned target are covered
			if(param.isTestCoverage() && groundTruth.has_taxon()) {
				update_coverage_statistics(cls, groundTruth, stats);
			}
		}
		else {
			stats.assign(cls.rank());
		}

		boolean showMapping = (param.getMapViewMode() == MetaCacheOptions.map_view_mode.all) ||
				(param.getMapViewMode() == MetaCacheOptions.map_view_mode.mapped_only && !cls.none());

		try{
			if(showMapping) {
				//print query header and ground truth
				if(param.isShowTopHits() || param.isShowAllHits()) {
					//show first contiguous string only
					int l = header.indexOf(' ');

					if (l != -1) {
						d.write(header, 0, l);
					/*
					auto oit = std::ostream_iterator<char>{os, ""};
					std::copy(header.begin(), header.begin() + l, oit);
					 */
					}
					else {
						d.write(header);
					}

					d.write(param.getOutSeparator());

					if(param.isShowGroundTruth()) {
						if(groundTruth.sequence_level()) {
							show_ranks_of_target(d, db, groundTruth.target(),
									param.getShowTaxaAs(), param.getLowestRank(),
									param.isShowLineage() ? param.getHighestRank() : param.getLowestRank());
						}
						else if(groundTruth.has_taxon()) {
							show_ranks(d, db, db.ranks(groundTruth.tax()),
									param.getShowTaxaAs(), param.getLowestRank(),
									param.isShowLineage() ? param.getHighestRank() : param.getLowestRank());
						}
						else {
							d.write("n/a");
						}

						d.write(param.getOutSeparator());
					}
				}

				//print results
				if(param.isShowAllHits()) {
					show_matches(d, db, hits, param.getLowestRank());
					d.write(param.getOutSeparator());
				}
				if(param.isShowTopHits()) {
					show_matches(d, db, tophits, param.getLowestRank());
					d.write(param.getOutSeparator());
				}
				if(param.isShowLocations()) {
					show_candidate_ranges(d, db, tophits);
					d.write(param.getOutSeparator());
				}
				show_classification(d, db, cls);

			}

			// BUSCA //HERE CHEMA mais abaixo
			if(this.param.isTestAlignment() && !cls.none()) {
				SequenceOrigin origin = this.db.origin_of_target(tophits.target_id(0));

				SequenceReader reader = new SequenceReader(origin.getFilename(), this.jsc);

				for(int i = 0; i < origin.getIndex(); ++i) {
					reader.next();
				}

				SequenceData finalSeq = reader.next();

				if(finalSeq != null) {
					String tgtSequ = finalSeq.getData();

				}

			}

		}
		catch(IOException e) {
			LOG.error("IOException in function process_database_answer: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function process_database_answer: "+ e.getMessage());
			System.exit(1);
		}

/*

//HERE CHEMA
		//optional alignment ------------------------------
		if(param.testAlignment && !cls.none()) {
			//check which of the top sequences has a better alignment
        const auto& origin = db.origin_of_target(tophits.target_id(0));

			try {
				//load candidate file and forward to sequence
				auto reader = make_sequence_reader(origin.filename);
				for(std::size_t i = 0; i < origin.index; ++i) reader->next();
				if(reader->has_next()) {
					auto tgtSequ = reader->next().data;
					auto subject = make_view_from_window_range(tgtSequ,
							tophits.window(0),
							db.target_window_stride());

					auto align = make_alignment(query1, query2, subject,
							needleman_wunsch_scheme{});

					stats.register_alignment_score(align.score);

					//print alignment to top candidate
					if(align.score > 0 && param.showAlignment) {
                    const auto w = db.target_window_stride();

						os << '\n'
								<< param.comment << "  score  " << align.score
								<< "  aligned to "
								<< origin.filename << " #" << origin.index
								<< " in range [" << (w * tophits.window(0).beg)
								<< ',' << (w * tophits.window(0).end) << "]\n"
								<< param.comment << "  query  " << align.query << '\n'
								<< param.comment << "  target " << align.subject;
					}
				}
			} catch(std::exception&) { }
		}

		if(showMapping) os << '\n';
		*/
	}

	public void show_ranks_of_target(BufferedWriter os, Database db, long tid, MetaCacheOptions.taxon_print_mode mode, Taxonomy.Rank lowest,
									 Taxonomy.Rank highest) {
		//since targets don't have their own taxonId, print their sequence id
		try {
			if(lowest == Taxonomy.Rank.Sequence) {
				if(mode != MetaCacheOptions.taxon_print_mode.id_only) {
					os.write("sequence:"+db.sequence_id_of_target(tid));

				} else {
					os.write(db.sequence_id_of_target(tid));
				}
			}

			if(highest == Taxonomy.Rank.Sequence) return;

			if(lowest == Taxonomy.Rank.Sequence) os.write(',');

			show_ranks(os, db, db.ranks_of_target((int)tid), mode, lowest, highest);
		}
		catch(IOException e) {
			LOG.error("IOException in function show_ranks_of_target: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function show_ranks_of_target: "+ e.getMessage());
			System.exit(1);
		}

	}


	public Classification sequence_classification(MatchesInWindow cand) {

		long wc = this.param.isWeightHitsWithWindows() ? cand.covered_windows() > 1 ? cand.covered_windows() - 1 : 1 : 1;

		//sum of top-2 hits < threshold => considered not classifiable
		if((cand.hits(0) + cand.hits(1)) < wc*param.getHitsMin()) {
			return new Classification();
		}

		//either top 2 are the same sequences with at least 'hitsMin' many hits
		//(checked before) or hit difference between these top 2 is above threshhold
		if( (cand.target_id(0) == cand.target_id(1))
				|| (cand.hits(0) - cand.hits(1)) >= wc*param.getHitsMin())
		{
			//return top candidate
			int tid = cand.target_id(0);
			return new Classification(tid, db.taxon_of_target((long)tid));
		}

		return new Classification(lowest_common_taxon(MatchesInWindow.maxNo, cand, (float)param.getHitsDiff(), param.getLowestRank(), param.getHighestRank()));


	}


	public Taxon lowest_common_taxon(int maxn, MatchesInWindow cand, float trustedMajority, Taxonomy.Rank lowestRank, Taxonomy.Rank highestRank) {
		if(lowestRank == null) {
			lowestRank = Taxonomy.Rank.subSpecies;
		}

		if(highestRank == null) {
			highestRank = Taxonomy.Rank.Domain;
		}

		if(maxn < 3 || cand.count() < 3) {
			Taxon tax = db.ranked_lca_of_targets(cand.target_id(0), cand.target_id(1));

			//classify if rank is below or at the highest rank of interest
			if(tax.getRank().ordinal() <= highestRank.ordinal()) return tax;

			HashMap<Long, Long> scores = new HashMap<Long, Long>(2*cand.count());

			for(Taxonomy.Rank r = lowestRank; r.ordinal() <= highestRank.ordinal(); r = Taxonomy.next_main_rank(r)) {

				//hash-count taxon id occurrences on rank 'r'
				int totalscore = 0;
				for(int i = 0, n = cand.count(); i < n; ++i) {
					//use target id instead of taxon if at sequence level
					long taxid = db.ranks_of_target_basic(cand.target_id(i))[r.ordinal()];
					if(taxid > 0) {
						long score = cand.hits(i);
						totalscore += score;

						Long it = scores.get(taxid);

						if(it != null) {
							scores.put(taxid, scores.get(taxid) + score);
						}
						else {
							scores.put(taxid, score);
						}

					}
				}

//            std::cout << "\n    " << taxonomy::rank_name(r) << " ";

				//determine taxon id with most votes
				long toptid = 0;
				long topscore = 0;

				for(Map.Entry<Long, Long> x : scores.entrySet()) {

//                std::cout << x.first << ":" << x.second << ", ";

					if(x.getValue() > topscore) {
						toptid = x.getKey();
						topscore = x.getValue();
					}
				}

				//if enough candidates (weighted by their hits)
				//agree on a taxon => classify as such
				if(topscore >= (totalscore * trustedMajority)) {

//                std::cout << "  => classified " << taxonomy::rank_name(r)
//                          << " " << toptid << '\n';

					return db.taxon_with_id(toptid);
				}

				scores.clear();
			}


		}

		//candidates couldn't agree on a taxon on any relevant taxonomic rank
		return null;

	}


	public void update_coverage_statistics(Classification result, Classification truth, ClassificationStatistics stats) {

		Long[] lin = this.db.ranks(truth.tax());


		//check if taxa are covered in DB
		for(long taxid : lin) {

			Taxonomy.Rank r = this.db.taxon_with_id(taxid).getRank();


			if(this.db.covers_taxon(taxid)) {
				if(r.ordinal() < result.rank().ordinal()) { //unclassified on rank
					stats.count_coverage_false_neg(r);
				} else { //classified on rank
					stats.count_coverage_true_pos(r);
				}
			}
			else {
				if(r.ordinal() < result.rank().ordinal()) { //unclassified on rank
					stats.count_coverage_true_neg(r);
				} else { //classified on rank
					stats.count_coverage_false_pos(r);
				}
			}
		}
	}

	public void show_ranks(BufferedWriter os, Database db, Long[] lineage,
						   MetaCacheOptions.taxon_print_mode mode, Taxonomy.Rank lowest, Taxonomy.Rank highest) {
		//one rank only
		try{
			if(lowest == highest) { // ordinal?
				long taxid = lineage[lowest.ordinal()];
				os.write(Taxonomy.rank_name(lowest) +  ':');
				if(mode != MetaCacheOptions.taxon_print_mode.id_only) {
					if(taxid > 1)
						os.write(db.taxon_with_id(taxid).getTaxonName());
					else
						os.write("n/a");
					if(mode != MetaCacheOptions.taxon_print_mode.name_only)
						os.write("(" + taxid + ")");
				}
				else {
					os.write(Long.toString(taxid));
				}
			}
			//range of ranks
			else {
				for(Taxonomy.Rank r = lowest; r.ordinal() <= highest.ordinal(); r = r.next()) {
					long taxid = lineage[r.ordinal()];
					if(taxid > 1) {
						Taxon taxon = db.taxon_with_id(taxid);
						if(taxon.getRank().ordinal() >= lowest.ordinal() && taxon.getRank().ordinal() <= highest.ordinal()) {
							os.write(taxon.rank_name() + ':');
							if(mode != MetaCacheOptions.taxon_print_mode.id_only) {
								os.write(taxon.getTaxonName());
								if(mode != MetaCacheOptions.taxon_print_mode.name_only) {
									os.write("(" + taxon.getTaxonId() + ")");
								}
							}
							else {
								os.write(Long.toString(taxon.getTaxonId()));
							}
						}
						if(r.ordinal() < highest.ordinal()) {
							os.write(',');
						}
					}
				}
			}
		}
		catch(IOException e) {
			LOG.error("IOException in function show_ranks: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function show_ranks: "+ e.getMessage());
			System.exit(1);
		}

	}


	//-------------------------------------------------------------------
	public void show_matches(BufferedWriter os, Database db, TreeMap<Location, Integer> matches,
					  Taxonomy.Rank lowest)	{
		if(matches.isEmpty()) {
			return;
		}
		try {
			if(lowest == Taxonomy.Rank.Sequence) {
				for(Map.Entry<Location, Integer> r : matches.entrySet()) {
					os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
							'/' + r.getKey().getWindowId()+
							':' + r.getValue() + ',');
				}
			}
			else {
				for(Map.Entry<Location, Integer> r : matches.entrySet()) {
					long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
					os.write(Long.toString(taxid) + ':' + r.getValue() + ',');
				}
			}
		}
		catch(IOException e) {
			LOG.error("IOException in function show_matches: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function show_matches: "+ e.getMessage());
			System.exit(1);
		}

	}

	public void show_matches(BufferedWriter os, Database db, MatchesInWindow matchesWindow,
							 Taxonomy.Rank lowest)	{

		TreeMap<Location, Integer> matches = matchesWindow.getMatches();

		if(matches.isEmpty()) {
			return;
		}
		try {
			if(lowest == Taxonomy.Rank.Sequence) {
				for(Map.Entry<Location, Integer> r : matches.entrySet()) {
					os.write(db.sequence_id_of_target(r.getKey().getTargetId())+
							'/' + r.getKey().getWindowId()+
							':' + r.getValue() + ',');
				}
			}
			else {
				for(Map.Entry<Location, Integer> r : matches.entrySet()) {
					long taxid = db.ranks_of_target(r.getKey().getTargetId())[lowest.ordinal()];
					os.write(Long.toString(taxid) + ':' + r.getValue() + ',');
				}
			}
		}
		catch(IOException e) {
			LOG.error("IOException in function show_matches: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function show_matches: "+ e.getMessage());
			System.exit(1);
		}

	}


	public void show_candidate_ranges(BufferedWriter os, Database db, MatchesInWindow cand) {

		int n = MatchesInWindow.maxNo;

		long w = db.getTargetWindowStride_();

		try {
			for(int i = 0; i < n; ++i) {
				os.write('[' + (w * cand.window(i).getBeg())
					+ ',' + (w * cand.window(i).getEnd() + "] "));
			}
		}
		catch(IOException e) {
			LOG.error("IOException in function show_matches: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function show_matches: "+ e.getMessage());
			System.exit(1);
		}
	}

	void show_classification(BufferedWriter os, Database db, Classification cls) {
		try {
			if(cls.sequence_level()) {
				Taxonomy.Rank rmax = param.isShowLineage() ? param.getHighestRank() : param.getLowestRank();

				show_ranks_of_target(os, db, cls.target(),
						param.getShowTaxaAs(), param.getLowestRank(), rmax);
			}
			else if(cls.has_taxon()) {
				if(cls.rank().ordinal() > param.getHighestRank().ordinal()) {
					os.write("--");
				}
				else {
					Taxonomy.Rank rmin = param.getLowestRank().ordinal() < cls.rank().ordinal() ? cls.rank() : param.getLowestRank();
					Taxonomy.Rank rmax = param.isShowLineage() ? param.getHighestRank() : rmin;

					show_ranks(os, db, db.ranks(cls.tax()),
							param.getShowTaxaAs(), rmin, rmax);
				}
			}
			else {
				os.write("--");
			}
		}
		catch(IOException e) {
			LOG.error("IOException in function show_matches: "+ e.getMessage());
			System.exit(1);
		}
		catch(Exception e) {
			LOG.error("Exception in function show_matches: "+ e.getMessage());
			System.exit(1);
		}
	}


}
