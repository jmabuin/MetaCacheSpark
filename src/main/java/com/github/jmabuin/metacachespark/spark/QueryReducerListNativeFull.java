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
public class QueryReducerListNativeFull implements Function2<List<MatchCandidate>, List<MatchCandidate>, List<MatchCandidate>> {

    //private static final Log LOG = LogFactory.getLog(QueryReducerListNative.class);

    private MetaCacheOptions options;

    public QueryReducerListNativeFull(MetaCacheOptions options) {
        this.options = options;
    }

    @Override
    public List<MatchCandidate> call(List<MatchCandidate> v1, List<MatchCandidate> v2) {

        v1.addAll(v2);

        return v1;
        /*CandidateGenerationRules rules = new CandidateGenerationRules();
        rules.setMaxCandidates(this.options.getProperties().getMaxCandidates() * 4);


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


            //for (MatchCandidate v: v1) {
            for(int i = 0; i< v1.size() && i < rules.getMaxCandidates(); ++i) {

                results.add(v1.get(i));


            }


        }

        return results;*/


    }

}
