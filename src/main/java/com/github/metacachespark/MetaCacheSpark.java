/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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
package com.github.metacachespark;

//import org.apache.spark.sql.SparkSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.*;

import java.io.Serializable;

/**
 * @author Jose M. Abuin
 * @brief MetaCacheSpark main class
 */
public class MetaCacheSpark implements Serializable {

	private static final Log LOG = LogFactory.getLog(MetaCacheSpark.class);

	public static void main(String[] args) {

		MetaCacheOptions newOptions = new MetaCacheOptions(args);

		if(newOptions.getMode() == MetaCacheOptions.Mode.HELP) {
			newOptions.printHelp();
		}
		else if(newOptions.getMode() == MetaCacheOptions.Mode.BUILD) {
			// Build mode entry point
			SparkSession sparkS = SparkSession
					.builder()
					.appName("MetaCacheSpark - Build")
					.getOrCreate();


			String buildArgs[] = newOptions.getOtherOptions();
			Build buildObject = new Build(buildArgs, sparkS);

			buildObject.buildDatabase();
			LOG.info("End of program ...");
			sparkS.close();
		}
		else {
			System.out.println("Not recognized option");
			newOptions.printHelp();
			//System.exit(1);
		}


		//System.exit(0);
	}

}
