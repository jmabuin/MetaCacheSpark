package com.github.jmabuin.metacachespark.spark;

import com.github.jmabuin.metacachespark.Location;
import com.github.jmabuin.metacachespark.LocationBasic;
import com.github.jmabuin.metacachespark.Sketch;
import com.github.jmabuin.metacachespark.database.HashMultiMapNative;
import com.github.jmabuin.metacachespark.io.SequenceData;
import com.github.jmabuin.metacachespark.io.SequenceFileReader;
import com.github.jmabuin.metacachespark.io.SequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by chema on 3/28/17.
 */
public class PartialQuery implements PairFlatMapFunction<Iterator<HashMultiMapNative>, Long, List<int[]>> {

	private static final Log LOG = LogFactory.getLog(PartialQuery.class);

	private String fileName;
	private long init;
	private int bufferSize;
	private long total;
	//private SequenceFileReader seqReader;
	private long readed;

	List<SequenceData> inputData;

	public PartialQuery(List<SequenceData> inputData, long init, int bufferSize, long total, long readed) {
		//this.seqReader = seqReader;
		//this.fileName = fileName;
		this.init = init;
		this.bufferSize = bufferSize;
		this.total = total;
		this.readed = readed;
		this.inputData = inputData;
	}

	@Override
	public Iterable<Tuple2<Long, List<int[]>>> call(Iterator<HashMultiMapNative> myHashMaps) {
/*
		List<Tuple2<Long, List<int[]>>> finalResults = new ArrayList<Tuple2<Long, List<int[]>>>();

		try{

			SequenceFileReader seqReader = new SequenceFileReader(this.fileName, this.readed);

			LOG.warn("Already readed characters: " + this.readed);
			//seqReader.skip(this.readed);


			SequenceData data = seqReader.next();



			ArrayList<Sketch> locations = new ArrayList<Sketch>();

			//HashMap<String, List<int[]>> results = new HashMap<String, List<int[]>>();

			//finalResults.add(results);

			long currentSequence = this.init;
			LOG.warn("[JMAbuin] Init at sequence: " + currentSequence);
			// Theoretically there is only one HashMap per partition
			HashMultiMapNative currentHashMap = myHashMaps.next();


			while((currentSequence < (this.init + this.bufferSize)) && (data != null)) {
				//LOG.warn("[JMAbuin] Processing sequence " + data.getHeader()+" :: "+data.getData());

				if(currentSequence >= this.init) {

					locations = seqReader.getSketch(data);

					List<int[]> queryResults = new ArrayList<int[]>();

					for(Sketch currentSketch: locations) {

						for(int location: currentSketch.getFeatures()) {

							int[] values = currentHashMap.get(location);

							if(values != null) {

								queryResults.add(values);

							}
						}

					}

					//results.put(data.getHeader(), queryResults);
					finalResults.add(new Tuple2<Long, List<int[]>>(currentSequence, queryResults));
				}


				data = seqReader.next();
				currentSequence++;
			}

			//seqReader.close();

			return finalResults;


		}
		catch(Exception e) {
			LOG.error("ERROR in PartialQuery: "+e.getMessage());
			System.exit(-1);
		}

		return finalResults;
*/

		List<Tuple2<Long, List<int[]>>> finalResults = new ArrayList<Tuple2<Long, List<int[]>>>();

		try{



			ArrayList<Sketch> locations = new ArrayList<Sketch>();

			//HashMap<String, List<int[]>> results = new HashMap<String, List<int[]>>();

			//finalResults.add(results);

			long currentSequence = this.init;
			//LOG.warn("[JMAbuin] Init at sequence: " + currentSequence);
			// Theoretically there is only one HashMap per partition
			while(myHashMaps.hasNext()){

				HashMultiMapNative currentHashMap = myHashMaps.next();


				for(SequenceData currentData: inputData){


					//if(currentSequence >= this.init) {

						locations = SequenceFileReader.getSketchStatic(currentData);

						List<int[]> queryResults = new ArrayList<int[]>();

						for(Sketch currentSketch: locations) {

							for(int location: currentSketch.getFeatures()) {

								int[] values = currentHashMap.get(location);

								if(values != null) {

									queryResults.add(values);

								}
							}

						}

						//results.put(data.getHeader(), queryResults);
						finalResults.add(new Tuple2<Long, List<int[]>>(currentSequence, queryResults));
					//}


					//data = seqReader.next();
					currentSequence++;
					locations.clear();
				}
			}

			//LOG.warn("[JMAbuin] Ending buffer " + currentSequence);

			//seqReader.close();

			return finalResults;


		}
		catch(Exception e) {
			LOG.error("ERROR in PartialQuery: "+e.getMessage());
			System.exit(-1);
		}

		return finalResults;
	}

}