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

/**
 * Created by chema on 3/3/17.
 */
public class MyCustomPartitioner extends Partitioner {



    private int numParts;

    public MyCustomPartitioner(int i) {
        this.numParts=i;
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
        long long_key =Integer.toUnsignedLong((Integer)key)+1; // Plus 1, as it can be zero

        return (int)(Math.abs(long_key)%this.numParts);

    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof MyCustomPartitionerStr)
        {
            MyCustomPartitioner partitionerObject = (MyCustomPartitioner)obj;
            if(partitionerObject.numPartitions() == this.numParts)
                return true;
        }

        return false;
    }

}
