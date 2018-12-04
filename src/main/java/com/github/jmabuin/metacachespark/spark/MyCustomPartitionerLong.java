package com.github.jmabuin.metacachespark.spark;

import org.apache.spark.Partitioner;

/**
 * Created by chema on 3/3/17.
 */
public class MyCustomPartitionerLong extends Partitioner {



    private int numParts;

    public MyCustomPartitionerLong(int i) {
        this.numParts = i;
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
        long long_key = (Long)key;

        return (int)(Math.abs(long_key)%this.numParts);

    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof MyCustomPartitionerLong)
        {
            MyCustomPartitionerLong partitionerObject = (MyCustomPartitionerLong)obj;
            if(partitionerObject.numPartitions() == this.numParts)
                return true;
        }

        return false;
    }

}
