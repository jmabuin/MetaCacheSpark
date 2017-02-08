package com.github.jmabuin.metacachespark;
import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Sequence implements Serializable {

    private String header;
    private String data;
    private int partitionId;
    private int fileId;
    private String fileName;
    private int taxid;

    public Sequence(String data, int partitionId, int fileId, String fileName, String header, int taxid) {
        this.data = data;
        this.partitionId = partitionId;
        this.fileId = fileId;
        this.fileName = fileName;
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
