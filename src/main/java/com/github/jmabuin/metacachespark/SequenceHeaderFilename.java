package com.github.jmabuin.metacachespark;

public class SequenceHeaderFilename {

    private String header;
    private String filename;

    public SequenceHeaderFilename(){

    }

    public SequenceHeaderFilename(String header, String filename) {
        this.header = header;
        this.filename = filename;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
