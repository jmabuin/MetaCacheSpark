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

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.TargetProperty;
import com.github.jmabuin.metacachespark.database.CandidateGenerationRules;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import com.github.jmabuin.metacachespark.database.Taxon;
import com.github.jmabuin.metacachespark.database.Taxonomy;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

/**
 * Created by chema on 3/30/17.
 */
public class QueryReducerListNative implements Function2<List<MatchCandidate>, List<MatchCandidate>, List<MatchCandidate>> {

    private static final Log LOG = LogFactory.getLog(QueryReducerListNative.class);

    private MetaCacheOptions options;
    /*private Taxonomy taxa_;
    private List<TargetProperty> targets_;

    public QueryReducerListNative(MetaCacheOptions options, Broadcast<Taxonomy> taxonomy_broadcast, Broadcast<List<TargetProperty>> targets_broadcast) {
        this.options = options;
        this.taxa_ = taxonomy_broadcast.value();
        this.targets_ = targets_broadcast.value();
    }*/

    public QueryReducerListNative(MetaCacheOptions options) {
        this.options = options;
    }

    @Override
    public List<MatchCandidate> call(List<MatchCandidate> v1, List<MatchCandidate> v2) {

        CandidateGenerationRules rules = new CandidateGenerationRules(this.options.getProperties());
        //rules.setMaxCandidates(this.options.getProperties().getMaxCandidates() * this.options.getProperties().getMaxCandidates());


        List<MatchCandidate> results = new ArrayList<>();

        v1.addAll(v2);

        //rules.setMaxCandidates((int)(v1.size() / 3));

        int local_min_hits = this.options.getHits_greater_than();//this.options.getProperties().getHitsMin() / 2;

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

            double threshold = v1.get(0).getHits() > local_min_hits ?
                    (v1.get(0).getHits() - local_min_hits) *
                            this.options.getProperties().getHitsDiffFraction() : 0;

            //for (int i = 0; (i < v1.size()) && (i < rules.getMaxCandidates()); ++i) {
            for (int i = 0; i < v1.size(); ++i) {
                if (v1.get(i).getHits() >= threshold) {
                    results.add(v1.get(i));
                }
                else {
                    break;
                }

            }

        }
        return results;


    }

    /*private Taxon get_taxon(int tgt) {
        //LOG.warn("Getting taxon for TgtId: " + entry.getTargetId());
        //LOG.warn("Target is: " + entry.getTargetId());
        //LOG.warn("Taxa from target in targets_ is: " + this.targets_.get(entry.getTargetId()).getTax());
        return this.taxa_.getTaxa_().get(this.targets_.get(tgt).getTax());
    }*/

}
