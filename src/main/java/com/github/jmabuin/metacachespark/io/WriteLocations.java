package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Locations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Created by chema on 3/22/17.
 */
public class WriteLocations implements Function2<Integer, Iterator<Locations>, Iterator<String>> {

    private static final Log LOG = LogFactory.getLog(WriteLocations.class);

    private String path;

    public WriteLocations(String path) {
        this.path = path;
    }

    @Override
    public Iterator<String> call(Integer partitionId, Iterator<Locations> values) {


        String filename = this.path+"/part-"+partitionId;
        ArrayList<String> returnValues = new ArrayList<String>();

        try {


            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            returnValues.add(filename);

            FSDataOutputStream outputStream = fs.create(new Path(filename));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

            // Write data
            StringBuffer currentLine = new StringBuffer();

            while(values.hasNext()) {

                Locations current_location = values.next();

                int key = current_location.getKey();
                List<LocationBasic> locations_values = current_location.getLocations();

                currentLine.append(key);
                currentLine.append(":");

                // Theoretically there is only one HashMap per partition
                //bw.write(String.valueOf(currentMap.size()));
                //bw.newLine();

                for(LocationBasic currentLocation: locations_values) {
                    currentLine.append(currentLocation.getTargetId());
                    currentLine.append(",");
                    currentLine.append(currentLocation.getWindowId());
                    currentLine.append(";");
                    //currentLine.append(",");
                }

                bw.write(currentLine.toString());
                bw.newLine();

                currentLine.delete(0, currentLine.toString().length());


            }


            bw.close();
            outputStream.close();

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
