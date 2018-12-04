package com.github.jmabuin.metacachespark.database;

import java.io.Serializable;

public class MatchCandidate implements Serializable {

    private Taxon tax;
    private int tgt;
    private long hits = 0;
    private int pos;
    private int pos_beg;
    private int pos_end;

    public MatchCandidate(){

        this.tax = null;
        this.hits = 0;
        this.pos = 0;
        this.pos_beg = 0;
        this.pos_end = 0;
        this.tgt = -1;

    }

    public MatchCandidate(Taxon tax, int hits) {
        this.tax = tax;
        this.hits = hits;

    }

    public MatchCandidate(int tgt, long hits, int pos, Taxon tax) {
        this.tgt = tgt;
        this.hits = hits;
        this.pos = pos;
        this.tax = tax;
    }

    public Taxon getTax() {
        return tax;
    }

    public void setTax(Taxon tax) {
        this.tax = tax;
    }

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getPos_beg() {
        return pos_beg;
    }

    public void setPos_beg(int pos_beg) {
        this.pos_beg = pos_beg;
    }

    public int getPos_end() {
        return pos_end;
    }

    public void setPos_end(int pos_end) {
        this.pos_end = pos_end;
    }

    public int getTgt() {
        return tgt;
    }

    public void setTgt(int tgt) {
        this.tgt = tgt;
    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        //if (other == this) return true;
        if (!(other instanceof MatchCandidate))return false;

        MatchCandidate otherMyClass = (MatchCandidate)other;

        if((otherMyClass.getTgt() == this.getTgt())) {
            return true;
        }
        else {
            return false;
        }

    }


    @Override
    public int hashCode() {

        return this.tgt;

    }
/*
    @Override
    public int compareTo(MatchCandidate other) {

        if (other == null) return 1;

        if(this.getHits() < other.getHits()) {
            return -1;
        }
        if(this.getHits() > other.getHits()) {
            return 1;
        }

        return 0;

    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        //if (other == this) return true;
        if (!(other instanceof MatchCandidate))return false;

        MatchCandidate otherMyClass = (MatchCandidate)other;

        if((otherMyClass.getTgt() == this.getTgt())) {
            return true;
        }
        else {
            return false;
        }

    }

    @Override
    public int hashCode() {

        return this.getTgt();

    }
*/
}
