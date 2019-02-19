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

package com.github.jmabuin.metacachespark;

import com.github.jmabuin.metacachespark.io.SequenceData;

import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Sequence implements Serializable {

    /*
    struct sequence {
        using data_type = std::string;
        index_type  index;       //number of sequence in file (+ offset)
        std::string header;      //meta information (FASTA >, FASTQ @)
        data_type   data;        //actual sequence data
        data_type   qualities;   //quality scores (FASTQ)
    };
     */

    // Normal fields
    private long index;
    private String header;
    private String data;
    private String qualities;

    //Extra fields
    private String seqId;
    private int taxid;
    private SequenceOrigin sequenceOrigin;

    //public Sequence(String data, String identifier, String fileName, int index, String header, int taxid) {
    public Sequence(long index, String header, String data, String qualities) {
        this.index = index;
        this.header = header;
        this.data = data;
        this.qualities = qualities;

        this.sequenceOrigin = new SequenceOrigin();
    }

    public Sequence(long index, String header, String data, String qualities, String filename) {
        this.index = index;
        this.header = header;
        this.data = data;
        this.qualities = qualities;

        this.sequenceOrigin = new SequenceOrigin();
        this.sequenceOrigin.setFilename(filename);
    }

    public Sequence(long index, String header, String qualities) {
        this.index = index;
        this.header = header;
        this.data = "";
        this.qualities = qualities;

        this.sequenceOrigin = new SequenceOrigin();
    }

    public Sequence(){
        this.data = "";

        this.sequenceOrigin = new SequenceOrigin();
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getData() {
        return data.toString();
    }

    public void setData(String data) {
        this.data = data;
    }

    //public void appendData(String new_data) {
    //    this.data.append(new_data);
    //}

    public String getQualities() {
        return qualities;
    }

    public void setQualities(String qualities) {
        this.qualities = qualities;
    }

    public String getSeqId() {
        return seqId;
    }

    public void setSeqId(String seqId) {
        this.seqId = seqId;
    }

    public int getTaxid() {
        return taxid;
    }

    public void setTaxid(int taxid) {
        this.taxid = taxid;
    }

    public SequenceOrigin getSequenceOrigin() {
        return sequenceOrigin;
    }

    public void setSequenceOrigin(SequenceOrigin sequenceOrigin) {
        this.sequenceOrigin = sequenceOrigin;
    }

    //public void reset_data(){
    //    this.data.delete(0, this.data.length());
    //}

    public void setOriginFilename(String filename) {
        this.sequenceOrigin.setFilename(filename);
    }

    public String getOriginFilename() {
        return this.sequenceOrigin.getFilename();
    }
}
