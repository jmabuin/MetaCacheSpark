package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.database.MatchesInWindowList;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducerListNative implements Function2<List<MatchCandidate>, List<MatchCandidate>, List<MatchCandidate>> {

    @Override
    public List<MatchCandidate> call(List<MatchCandidate> v1, List<MatchCandidate> v2) {

        long best = 0;
        long best_v1 = 0;
        long best_v2 = 0;

        if (!v1.isEmpty()) {
            best_v1 = v1.get(0).getHits();

        }

        if (!v2.isEmpty()) {
            best_v2 = v2.get(0).getHits();
        }

        double threshold = 0.0;


        if (best_v1 >= best_v2 && best_v1 > 0) {
            threshold = v1.get(0).getHits() > 1 ?
                    (v1.get(0).getHits() - 1) * 1 : 0;
        }
        else if (best_v2 > 0){
            threshold = v2.get(0).getHits() > 1 ?
                    (v2.get(0).getHits() - 1) * 1 : 0;
        }





        v1.addAll(v2);


        // Sort candidates in DESCENDING order according number of hits
        Collections.sort(v1, new Comparator<MatchCandidate>() {
            public int compare(MatchCandidate o1,
                               MatchCandidate o2)
            {

                if (o1.getHits() < o2.getHits()) {
                    return 1;
                }

                if (o1.getHits() > o2.getHits()) {
                    return -1;
                }

                return 0;

            }
        });

        List<MatchCandidate> results = new ArrayList<>();

        for (MatchCandidate v: v1) {

            if (v.getHits() >= threshold) {
                results.add(v);
            }

        }

        return results;

    }

}
