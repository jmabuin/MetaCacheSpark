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
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

/**
 * Created by chema on 3/28/17.
 */
public class ReadHashMapGuava implements Function2<Integer, Iterator<String>, Iterator<HashMultimap<Integer, LocationBasic>>> {

    private static final Log LOG = LogFactory.getLog(ReadHashMapGuava.class);

    private String path;



    @Override
    public Iterator<HashMultimap<Integer, LocationBasic>> call(Integer partitionId, Iterator<String> values) {

        String filename;
        String onlyFileName;
        ArrayList<HashMultimap<Integer, LocationBasic>> returnValues = new ArrayList<HashMultimap<Integer, LocationBasic>>();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            while (values.hasNext()) {


                filename = values.next();
                String parts[] = filename.split("/");
                onlyFileName = parts[parts.length-1];

                LOG.info("Processing file to insert into Java hashmap: " + filename+" <> "+onlyFileName);

                //HashMultimap<Integer, LocationBasic> newMap = null;//new HashMap<Integer, List<LocationBasic>>();

                fs.copyToLocalFile(new Path(filename), new Path(onlyFileName));
                LOG.info("File copied to local: " +onlyFileName);
                LOG.info("Creating object.");
                FileInputStream fileIn = new FileInputStream(onlyFileName);
                ObjectInputStream in = new ObjectInputStream(fileIn);

                Map<Integer, Set<LocationBasic>> map = (Map<Integer, Set<LocationBasic>>) in.readObject();

                HashMultimap<Integer, LocationBasic> newMap = HashMultimap.create();

                for (Map.Entry<Integer, Set<LocationBasic>> entry :
                        map.entrySet()) {
                    newMap.putAll(entry.getKey(), entry.getValue());
                }

                //Map<Integer, Collection<LocationBasic>> tmp_map = (Map<Integer, Collection<LocationBasic>>) in.readObject();
                //HashMultimap<Integer, LocationBasic> newMap = HashMultimap.create();

                in.close();
                fileIn.close();

                LOG.info("Object created");

                //newMap.read(onlyFileName);

                returnValues.add(newMap);

                File file = new File(onlyFileName);

                file.delete();
            }

            return returnValues.iterator();
        }
        catch(ClassCastException e) {
            LOG.error("Could not read file because of Cast error in writeSid2gid.");
            e.printStackTrace();
        }

        catch (IOException e) {
            LOG.error("Could not read file because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not read file because of General error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

        return returnValues.iterator();

    }

}
