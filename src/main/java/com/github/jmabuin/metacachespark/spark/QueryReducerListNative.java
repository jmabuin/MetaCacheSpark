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

package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.database.CandidateGenerationRules;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducerListNative implements Function2<List<MatchCandidate>, List<MatchCandidate>, List<MatchCandidate>> {

    //private static final Log LOG = LogFactory.getLog(QueryReducerListNative.class);

    private MetaCacheOptions options;

    public QueryReducerListNative(MetaCacheOptions options) {
        this.options = options;
    }

    @Override
    public List<MatchCandidate> call(List<MatchCandidate> v1, List<MatchCandidate> v2) {

        //long initTime = System.nanoTime();

        CandidateGenerationRules rules = new CandidateGenerationRules();
        rules.setMaxCandidates(this.options.getProperties().getMaxCandidates());

        v1.addAll(v2);

        List<MatchCandidate> results = new ArrayList<>();

        if (! v1.isEmpty()) {

            // Sort candidates in DESCENDING order according number of hits
            v1.sort(new Comparator<MatchCandidate>() {
                public int compare(MatchCandidate o1,
                                   MatchCandidate o2) {

                    if (o1.getHits() < o2.getHits()) {
                        return 1;
                    }

                    if (o1.getHits() > o2.getHits()) {
                        return -1;
                    }

                    return 0;

                }
            });

            double threshold = v1.get(0).getHits() > this.options.getProperties().getHitsMin() ?
                    (v1.get(0).getHits() - this.options.getProperties().getHitsMin()) *
                            this.options.getProperties().getHitsDiffFraction() : 0;


            //for (MatchCandidate v: v1) {
            for(int i = 0; i< v1.size() && i < rules.getMaxCandidates(); ++i) {

                MatchCandidate v = v1.get(i);
                if (v.getHits() > threshold) {
                    results.add(v);
                }
                else {
                    break;
                }

            }


        }



/*
        List<MatchCandidate> results = new ArrayList<>();

        long best_v1 = 0;
        long best_v2 = 0;
        long best_all = 0;

        if (!v2.isEmpty()){
            best_v2 = v2.get(0).getHits();
        }

        if (!v1.isEmpty()) {
            best_v1 = v1.get(0).getHits();
        }

        if (best_v1 > best_v2) {
            best_all = best_v1;
        }
        else {
            best_all = best_v2;
        }

        double threshold = best_all > this.options.getProperties().getHitsMin() ?
                (best_all - this.options.getProperties().getHitsMin()) *
                        this.options.getProperties().getHitsDiffFraction() : 0;



        for (MatchCandidate v: v1) {

            if (v.getHits() > threshold) {
                results.add(v);
            }
            else {
                break;
            }

        }

        for (MatchCandidate v: v2) {

            if (v.getHits() > threshold) {
                results.add(v);
            }
            else {
                break;
            }

        }

        if (!results.isEmpty()) {
            // Sort candidates in DESCENDING order according number of hits
            //Collections.sort(v1, new Comparator<MatchCandidate>() {
            results.sort(new Comparator<MatchCandidate>() {
                public int compare(MatchCandidate o1,
                                   MatchCandidate o2) {

                    if (o1.getHits() < o2.getHits()) {
                        return 1;
                    }

                    if (o1.getHits() > o2.getHits()) {
                        return -1;
                    }

                    return 0;

                }
            });

            MatchCandidate best = results.get(0);
            //this.best.setHits(list.get(list.size()-1).getValue());
            //this.lca = this.best.getTax();

            if (best.getHits() < this.options.getProperties().getHitsMin()) {
                return new ArrayList<MatchCandidate>();
            }
        }


        //long endTime = System.nanoTime();

        //LOG.warn("Time spent in reduction phase is: " + ((endTime - initTime) / 1e9) + " seconds");
*/
        return results;

}

}
