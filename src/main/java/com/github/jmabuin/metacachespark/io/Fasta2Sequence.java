package com.github.jmabuin.metacachespark.io;

import com.github.jmabuin.metacachespark.Sequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chema on 3/28/17.
 */
public class Fasta2Sequence implements FlatMapFunction<Iterator<String>, Sequence> {


    private static final Log LOG = LogFactory.getLog(Fasta2Sequence.class);
    private HashMap<String, Long> sequ2taxid;

    public Fasta2Sequence(HashMap<String, Long> sequ2taxid) {
        this.sequ2taxid = sequ2taxid;
    }

    @Override
    public Iterator<Sequence> call(Iterator<String> fileNames) {

        List<Sequence> returnValues = new ArrayList<Sequence>();

        StringBuffer header = new StringBuffer();
        StringBuffer data = new StringBuffer();
        StringBuilder content = new StringBuilder();

        try {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            long sequence_number = 0;

            while(fileNames.hasNext()) {



                content.delete(0, content.toString().length());

                String fileName = fileNames.next();



                if(!fileName.contains("assembly_summary")) {

                    String key = fileName;
                    FSDataInputStream inputStream = fs.open(new Path(fileName));

                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

                    String currentLine;

                    while ((currentLine = br.readLine()) != null) {

                        if (currentLine.startsWith(">")) {
                            if(!header.toString().isEmpty()) {
                                //returnedValues.add(new Sequence(data.toString(), "", currentFile.toString(), -1,
                                //		header.toString(), -1));
                                LOG.warn("Adding sequence : " + header.toString());
                                returnValues.add(new Sequence(sequence_number, header.toString(), data.toString(), ""));
                                sequence_number++;
                            }

                            header.delete(0,header.length());
                            header.append(currentLine.substring(1));
                            //data = "";
                            data.delete(0,data.length());
                        }
                        else {

                            //data = data + newLine;
                            data.append(currentLine);

                        }

                    }

                    br.close();
                    inputStream.close();

                    //LOG.warn("Reading file: "+fileName+" - " + content.length());

                    if ((!data.toString().isEmpty()) && (!header.toString().isEmpty())) {
                        LOG.warn("Adding last sequence : " + header.toString());
                        returnValues.add(new Sequence(sequence_number, header.toString(), data.toString(), ""));

                    }


                    int currentIndexNumber = 0;

                    for (Sequence currentSequence : returnValues) {
                        //LOG.info("Processing file: "+ currentFile);


                        String seqId = SequenceReader.extract_sequence_id(currentSequence.getHeader());
                        String fileIdentifier = SequenceReader.extract_sequence_id(fileName);

                        //make sure sequence id is not empty,
                        //use entire header if neccessary
                        if (seqId.isEmpty()) {
                            if (!fileIdentifier.isEmpty()) {
                                seqId = fileIdentifier;
                            } else {
                                seqId = currentSequence.getHeader();
                            }
                        }

                        //targets need to have a sequence id
                        //look up taxon id
                        int taxid = 0;

                        if (!sequ2taxid.isEmpty()) {
                            Long it = sequ2taxid.get(seqId);
                            if (it != null) {
                                taxid = it.intValue();
                            } else {
                                it = sequ2taxid.get(fileIdentifier);
                                if (it != null) {
                                    taxid = it.intValue();
                                }
                            }
                        }
                        //no valid taxid assigned -> try to find one in annotation

                        if (taxid <= 0) {
                            taxid = SequenceReader.extract_taxon_id(currentSequence.getHeader()).intValue();

                        }

                        currentSequence.setTaxid(taxid);
                        currentSequence.getSequenceOrigin().setIndex(currentIndexNumber);
                        currentSequence.getSequenceOrigin().setFilename(fileName);
                        currentSequence.setSeqId(seqId);

                        currentIndexNumber++;
                    }


                }



            }


            return returnValues.iterator();
        }
        catch(IOException e) {
            LOG.error("Could not acces to HDFS");
            System.exit(-1);
        }


        return returnValues.iterator();

    }

}
