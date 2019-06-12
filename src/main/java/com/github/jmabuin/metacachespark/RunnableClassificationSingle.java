package com.github.jmabuin.metacachespark;

import com.github.jmabuin.metacachespark.database.Classification;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.database.MatchesInWindowList;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;

import java.util.List;
import java.util.Map;

class RunnableClassificationSingle implements Runnable {
    private Thread t;
    private Query query_obj;
    private List<MatchCandidate> hits;
    //private Classification[] classifications;
    private Map<Integer, Classification> classifications;
    private String threadName;
    private String header;
    //private int size_data1;
    //private int size_data2;
    private int targetWindowStride;
    private MetaCacheOptions options;
    //private int currentSequence;
    //private Database db;
    private String f1;
    private int startRead;
    private int bufferSize;
    private List<Integer> data;
    private List<String> headers;

    RunnableClassificationSingle(Query query_obj, Map<Integer, Classification> classifications, String f1, String name,
                                 int startRead, int bufferSize, int targetWindowStride,
                                 MetaCacheOptions options,
                                 List<Integer> data, List<String> headers) {
        this.query_obj = query_obj;
        this.classifications = classifications;
        //this.hits = hits;
        this.threadName = name;
        //this.header =header;
        this.startRead = startRead;
        this.bufferSize = bufferSize;
        this.targetWindowStride = targetWindowStride;
        this.options = options;
        //this.currentSequence = currentSequence;
        //this.db = db;
        this.f1 = f1;
        this.data = data;
        this.headers = headers;
    }

    public void run() {
        //System.out.println("Running " +  threadName );
        try {

            // Get corresponding hits for this buffer
            Map<Long, List<MatchCandidate>> hits;

            hits = this.query_obj.db.accumulate_matches_single(f1,
                    startRead, bufferSize);

            for(int i = 0;  i < hits.size() ; i++) {



                long current_read = startRead + i;
                int current_read_int = startRead + i;

                //Theoretically, number of sequences in data is the same as number of hits
                int data_len = data.get(i);
                String header_str = headers.get(i);


                List<MatchCandidate> currentHits = hits.get(current_read);

                if (header_str.isEmpty()) return;


                //preparation -------------------------------
                Classification groundTruth = new Classification();

                if (this.options.getProperties().isTestPrecision() ||
                        (this.options.getProperties().getMapViewMode() != EnumModes.map_view_mode.none && this.options.getProperties().isShowGroundTruth()) ||
                        (this.options.getProperties().getExcludedRank() != Taxonomy.Rank.none)) {

                    groundTruth = this.query_obj.db.ground_truth(header);

                }

                //clade exclusion
                if (this.options.getProperties().getExcludedRank() != Taxonomy.Rank.none && groundTruth.has_taxon()) {
                    long exclTaxid = this.query_obj.db.ranks(groundTruth.tax())[this.options.getProperties().getExcludedRank().ordinal()];
                    this.query_obj.remove_hits_on_rank(this.options.getProperties().getExcludedRank(), exclTaxid); //Todo: Look what this function does
                }

                //classify ----------------------------------
                long numWindows = (2 + Math.max(data_len, this.options.getProperties().getInsertSizeMax()) / this.targetWindowStride);

                //LOG.warn("Starting classification");
                MatchesInWindowList tophits = new MatchesInWindowList(currentHits, (int) numWindows, this.query_obj.db.getTargets_(), this.query_obj.db.getTaxa_(), this.options);
                //tophits.print_top_hits();
                Classification cls = this.query_obj.sequence_classification(tophits);

                //this.classifications[this.currentSequence] = cls;

                this.classifications.put(current_read_int, cls);
            }
        }
        catch (Exception e) {
            System.out.println("Thread " +  threadName + " interrupted.");
            e.printStackTrace();
        }
        //System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start () {
        //System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }

}