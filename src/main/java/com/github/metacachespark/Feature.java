package com.github.metacachespark;

import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Feature implements Serializable {

    private int key;
    private int partitionId;
    private int fileId;
    private int windowId;
    //private String header;
    //private int taxid;

    public Feature(int key, int partitionId, int fileId, int windowId) {
        this.key = key;
        this.partitionId = partitionId;
        this.fileId = fileId;
        //this.header = header;
        //this.taxid = taxid;
    }

    public Feature() {

        this.key = Integer.MAX_VALUE;
        this.partitionId = -1;
        this.fileId = -1;
        this.windowId = -1;
        //this.header = "";
        //this.taxid = -1;

    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
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
/*
    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public int getTaxid() {
        return taxid;
    }

    public void setTaxid(int taxid) {
        this.taxid = taxid;
    }*/
}
