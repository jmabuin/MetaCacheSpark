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

import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by chema on 3/28/17.
 */
public class ReadHashMapNative implements Function2<Integer, Iterator<String>, Iterator<HashMultiMapNative>> {

    private static final Log LOG = LogFactory.getLog(ReadHashMapNative.class);

    private String path;



    @Override
    public Iterator<HashMultiMapNative> call(Integer partitionId, Iterator<String> values) {
        String filename;
        String onlyFileName;
        ArrayList<HashMultiMapNative> returnValues = new ArrayList<HashMultiMapNative>();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            while (values.hasNext()) {


                filename = values.next();
                String parts[] = filename.split("/");
                onlyFileName = parts[parts.length-1];

                LOG.info("Processing file to insert into native hashmap: " + filename+" <> "+onlyFileName);

                HashMultiMapNative newMap = new HashMultiMapNative(254);

                fs.copyToLocalFile(new Path(filename), new Path(onlyFileName));

                newMap.read(onlyFileName);

                LOG.warn("Number of features in this partition: " + newMap.size());

                returnValues.add(newMap);

                File file = new File(onlyFileName);

                file.delete();
            }

            return returnValues.iterator();
        }
        catch (IOException e) {
            LOG.error("Could not read file because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not read file because of IO error in writeSid2gid.");
            e.printStackTrace();
            //System.exit(1);
        }

        return returnValues.iterator();

    }

}
