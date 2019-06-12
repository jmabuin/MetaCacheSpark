package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CopyFilesToLocal implements FlatMapFunction<Iterator<HashMultiMapNative>, String> {

    private static final Log LOG = LogFactory.getLog(CopyFilesToLocal.class);

    private String fileName;
    private String fileName2;

    public CopyFilesToLocal(String file_name, String file_name2) {

        this.fileName = file_name;
        this.fileName2 = file_name2;
    }

    @Override
    public Iterator<String> call(Iterator<HashMultiMapNative> hashMultiMapNativeIterator) throws Exception {

        List<String> result = new ArrayList<>();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            if (!this.fileName.isEmpty()) {
                Path hdfs_file_path = new Path(this.fileName);
                Path local_file_path = new Path(hdfs_file_path.getName());

                File tmp_file = new File(local_file_path.getName());

                if (!tmp_file.exists()) {
                    fs.copyToLocalFile(hdfs_file_path, local_file_path);
                    result.add("File " + local_file_path.getName() + " copied");
                } else {
                    result.add("File " + local_file_path.getName() + " already exists. Not copying.");
                }
            }

            if (!this.fileName2.isEmpty()) {
                Path hdfs_file_path2 = new Path(this.fileName2);
                Path local_file_path2 = new Path(hdfs_file_path2.getName());

                File tmp_file2 = new File(local_file_path2.getName());

                if (!tmp_file2.exists()) {
                    fs.copyToLocalFile(hdfs_file_path2, local_file_path2);
                    result.add("File " + local_file_path2.getName() + " copied");
                } else {
                    result.add("File " + local_file_path2.getName() + " already exists. Not copying.");
                }
            }

        }
        catch (Exception e) {
            LOG.error("ERROR in CopyFilesToLocal: " + e.getMessage());
            System.exit(-1);
        }



        return result.iterator();

    }
}
