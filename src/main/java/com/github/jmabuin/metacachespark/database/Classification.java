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

/**
 * Created by chema on 2/9/17.
 */
public class Classification {

    private long tid_;
    private Taxon tax_;

    public Classification(long tid_, Taxon tax_){

        this.tid_ = tid_;
        this.tax_ = tax_;
    }

    public Classification( Taxon tax) {
        this.tid_ = Database.invalid_target_id();
        this.tax_ = tax;
    }

    public Classification() {
        this.tid_ = Database.invalid_target_id();
        this.tax_ = new Taxon();//null;
    }

    public boolean has_taxon() {
        return tax_ != null;
    }

    public boolean sequence_level() {
        //return tid_ != Database.invalid_target_id();

        return this.tax_.getRank() == Taxonomy.Rank.Sequence;
    }

    public boolean none() {
        return !sequence_level() && !has_taxon();
    }

    public long target() {
        return tid_;
    }
    public Taxon tax() {
        return tax_;
    }

    public Taxonomy.Rank rank()  {
        return this.sequence_level() ? Taxonomy.Rank.Sequence : (this.has_taxon() ? tax_.getRank() : Taxonomy.Rank.none);
    }

    public void print() {
        if (this.tax_!= null) {
            System.out.println("Name: " + this.tax_.getTaxonName());
            System.out.println("Taxon level : " + this.tax_.getRank().name());
            System.out.println("ID: " + this.tax_.getTaxonId() + ", Parent ID: " + this.tax_.getParentId());
        }
        else {
            System.out.println("This classification taxon information is null");
        }

    }
}
