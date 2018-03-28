package com.github.jmabuin.metacachespark;
import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Sketch implements Serializable {

    private int[] features;
    private String header;
    private String sequence;
    //private int maxFeature = Integer.MAX_VALUE;

    public Sketch() {

        this.features = new int[MCSConfiguration.sketchSize];


    }

	public Sketch(String header, String sequence, int[] features) {

		this.features = features;
		this.header = header;
		this.sequence = sequence;

	}

    public int[] getFeatures() {
        return features;
    }

    public void setFeatures(int[] features) {
        this.features = features;
    }

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

    /*public boolean insert(Location feature) {

        if(feature.getKey() > this.maxFeature) {
            return false;
        }
        else {

            int i;

            for(i = 0;i < this.features.length-1;i++){
                if(this.features[i].getKey() > feature.getKey())
                    break;
            }

            if(i >=  this.features.length) {
                return false;
            }

            for(int k=this.features.length-2; k>=i; k--){
                this.features[k+1]=this.features[k];
            }
            this.features[i] = feature;
            this.maxFeature = features[features.length-1].getKey();

            return true;

        }

    }*/


}
