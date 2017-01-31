package com.github.metacachespark;

import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Sequence implements Serializable {

    private String data;
    private int partitionId;
    private long fileId;
    private String currentFile;
    private String header;
    private int taxid;

    public Sequence(String data, int partitionId, long fileId, String currentFile, String header, int taxid) {
        this.data = data;
        this.partitionId = partitionId;
        this.fileId = fileId;
        this.currentFile = currentFile;
        this.header = header;
        this.taxid = taxid;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public String getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(String currentFile) {
        this.currentFile = currentFile;
    }

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
    }
}
