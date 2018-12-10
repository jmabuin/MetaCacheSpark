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

package com.github.jmabuin.metacachespark.database;

public class CandidateGenerationRules {

    /*
    //maximum length of contiguous window range
    window_id maxWindowsInRange = 3;

    //maximum number of candidates to be generated
    std::size_t maxCandidates = std::numeric_limits<std::size_t>::max(); // 18446744073709551615

    //list only the best candidate of a taxon on rank
    taxon_rank mergeBelow = taxon_rank::Sequence;
     */

    private int maxWindowsInRange;
    private long maxCandidates;
    private Taxonomy.Rank mergeBelow;

    public CandidateGenerationRules() {
        this.maxWindowsInRange = 3;
        this.maxCandidates = Long.MAX_VALUE;
        this.mergeBelow = Taxonomy.rank_from_name("sequence");

    }


    public int getMaxWindowsInRange() {
        return maxWindowsInRange;
    }

    public void setMaxWindowsInRange(int maxWindowsInRange) {
        this.maxWindowsInRange = maxWindowsInRange;
    }

    public long getMaxCandidates() {
        return maxCandidates;
    }

    public void setMaxCandidates(long maxCandidates) {
        this.maxCandidates = maxCandidates;
    }

    public Taxonomy.Rank getMergeBelow() {
        return mergeBelow;
    }

    public void setMergeBelow(Taxonomy.Rank mergeBelow) {
        this.mergeBelow = mergeBelow;
    }
}
