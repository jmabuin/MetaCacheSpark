package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.google.common.collect.HashMultimap;
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
                HashMultimap<Integer, LocationBasic> currentMap = values.next();
                out.writeObject(currentMap);

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
