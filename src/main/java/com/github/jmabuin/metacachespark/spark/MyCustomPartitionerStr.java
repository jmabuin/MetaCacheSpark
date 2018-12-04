package com.github.jmabuin.metacachespark.spark;

import org.apache.spark.Partitioner;

import java.util.HashMap;

/**
 * Created by chema on 3/3/17.
 */
public class MyCustomPartitionerStr extends Partitioner {



    private int numParts;
    private HashMap<String, Integer> values;

    public MyCustomPartitionerStr(int i, HashMap<String, Integer> values) {
        this.numParts=i;
        this.values = values;
    }

    @Override
    public int numPartitions()
    {
        return this.numParts;
    }

    @Override
    public int getPartition(Object key){

        //partition based on the first character of the key...you can have your logic here !!
        //return (Math.abs((Integer)key))%this.numParts;
        String str_key = (String) key;

        int partition_number = this.values.get(str_key);

        return partition_number;

    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof MyCustomPartitioner)
        {
            MyCustomPartitionerStr partitionerObject = (MyCustomPartitionerStr)obj;
            if(partitionerObject.numPartitions() == this.numParts)
                return true;
        }

        return false;
    }

}
