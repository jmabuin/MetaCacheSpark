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
