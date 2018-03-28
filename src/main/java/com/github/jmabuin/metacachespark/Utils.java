package com.github.jmabuin.metacachespark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by chema on 1/23/17.
 */
public class Utils {

	private static final Log LOG = LogFactory.getLog(Utils.class);

	public static void show_progress_indicator(float done, int totalLength) {
		int m = (int)((totalLength - 7) * done);
		String process = "";
		process = process +"\r[";


		for(int j = 0; j < m; ++j) process = process + '=';
		process = process + ">";

		m = totalLength - 7 - m;
		for(int j = 0; j < m; ++j) process = process + ' ';

		process = process +  "] " + (int)(100 * done) + "%";

		LOG.info(process);
	}

	public static void show_progress_indicator(float done) {

		show_progress_indicator(done, 80);
	}

}
