/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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

    private SequenceData sequenceData;
    private int partitionId;
    private int fileId;
    private String fileName;
    private int taxid;

    public Sequence(String data, int partitionId, int fileId, String fileName, String header, int taxid) {
        this.sequenceData = new SequenceData(header, data, "");

        this.partitionId = partitionId;
        this.fileId = fileId;
        this.fileName = fileName;
        this.taxid = taxid;
    }

    public String getData() {
        return sequenceData.getData();
    }

    public void setData(String data) {
        this.sequenceData.setData(data);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getHeader() {
        return this.sequenceData.getHeader();
    }

    public void setHeader(String header) {
        this.sequenceData.setHeader(header);
    }

    public String getQuality() {
        return this.sequenceData.getQuality();
    }

    public void setQuality(String quality) {
        this.sequenceData.setQuality(quality);
    }

    public int getTaxid() {
        return taxid;
    }

    public void setTaxid(int taxid) {
        this.taxid = taxid;
    }
}