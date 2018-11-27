package com.github.jmabuin.metacachespark.database;

public class MatchCandidate {

    private Taxon tax;
    private int hits = 0;
    private int pos;

    public MatchCandidate(){

        this.tax = null;
        this.hits = 0;
        this.pos = 0;

    }

    public MatchCandidate(Taxon tax, int hits) {
        this.tax = tax;
        this.hits = hits;

    }

    public Taxon getTax() {
        return tax;
    }

    public void setTax(Taxon tax) {
        this.tax = tax;
    }

    public int getHits() {
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }
}
