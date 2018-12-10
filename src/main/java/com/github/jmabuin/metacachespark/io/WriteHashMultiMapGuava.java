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

package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.util.*;

/**
 * Created by chema on 3/22/17.
 */
public class WriteHashMultiMapGuava implements Function2<Integer, Iterator<HashMultimap<Integer, LocationBasic>>, Iterator<String>> {

    private static final Log LOG = LogFactory.getLog(WriteHashMultiMapGuava.class);

    private String path;

    public WriteHashMultiMapGuava(String path) {
        this.path = path;
    }

    @Override
    public Iterator<String> call(Integer partitionId, Iterator<HashMultimap<Integer, LocationBasic>> values) {


        String filename = this.path+"/part-"+partitionId;
        String local_filename = "part-"+partitionId;
        ArrayList<String> returnValues = new ArrayList<String>();

        try {


            FileOutputStream fileOut = new FileOutputStream(local_filename);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            while(values.hasNext()) {
                //Map<Integer, Collection<LocationBasic>> currentMap = values.next().asMap();
                HashMultimap<Integer, LocationBasic> currentMap = values.next();
                //out.writeObject(currentMap);

                Map<Integer, Set<LocationBasic>> map = Maps.newHashMap();
                for (Map.Entry<Integer, Collection<LocationBasic>> entry :
                        currentMap.asMap().entrySet()) {
                    map.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
                }

                out.writeObject(map);

            }

            out.close();
            fileOut.close();

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            fs.copyFromLocalFile(new Path(local_filename), new Path(filename));

            returnValues.add(filename);


            return returnValues.iterator();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ filename+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ filename+ " because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

        return returnValues.iterator();

    }

}
