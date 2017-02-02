package com.github.metacachespark;

import java.io.Serializable;

/**
 * Created by jabuinmo on 31.01.17.
 */
public class Sketch implements Serializable {

    private Feature[] features;
    private int maxFeature = Integer.MAX_VALUE;

    public Sketch() {

        this.features = new Feature[MCSConfiguration.sketchSize];

        for(int i = 0; i< this.features.length; i++) {
            this.features[i] = new Feature();
            //currentFeature.setKey(this.maxFeature);
        }

    }

    public Feature[] getFeatures() {
        return features;
    }

    public void setFeatures(Feature[] features) {
        this.features = features;
    }

    public boolean insert(Feature feature) {

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

    }


}