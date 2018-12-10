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
