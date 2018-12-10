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
import com.github.jmabuin.metacachespark.database.Taxon;
import com.github.jmabuin.metacachespark.database.Taxonomy;

import java.io.*;
import java.util.Base64;

/**
 * Created by chema on 1/13/17.
 */
public class IOSerialize {


	public enum DataTypes {INT, STRING, RANK, LONG, TAXON}

	public static void write_binary(OutputStream os, Taxon T) {

		try {
			os.write(T.toString().getBytes());

		}
		catch(IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(1);

		}
	}

	public static void write_binary(ObjectOutputStream oos, Object T, DataTypes t) {
		try {

			switch (t) {
				case INT:
					int intValueToWrite = (Integer) T;
					oos.writeInt(intValueToWrite);
					break;
				case STRING:
					String strValueToWrite = (String) T;
					//Long n = new Long(strValueToWrite.length());
					//oos.writeLong(n);
					oos.writeUTF(strValueToWrite);
					break;
				case RANK:
					Taxonomy.Rank rankValueToWrite = (Taxonomy.Rank) T;
					oos.writeObject(rankValueToWrite);
					break;
				case LONG:
					long longValueToWrite = (Long) T;
					oos.writeLong(longValueToWrite);
					break;
				case TAXON:
					Taxon taxonValueToWrite = (Taxon) T;
					oos.writeObject(taxonValueToWrite);
					break;

				default:
					break;
			}


		}
		catch(IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(1);

		}

	}

	public static void write_binary(OutputStream os, String str) {

		long n = str.length();
		try {
			os.write(Long.toString(n).getBytes());
			os.write(str.getBytes());
		}
		catch(IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(1);

		}

	}


	public static Object read_binary(ObjectInputStream ois,  DataTypes t) {

		Object T = null;

		try {

			switch (t) {
				case INT:
					int intValueRead = ois.readInt();
					T = intValueRead;
					break;
				case STRING:

					String strValueRead = ois.readUTF();
					T = strValueRead;
					break;
				case RANK:
					Taxonomy.Rank rankValueToRead = (Taxonomy.Rank) ois.readObject();
					T = rankValueToRead;
					break;
				case LONG:
					long longValueToRead = (Long) ois.readLong();
					T = longValueToRead;
				case TAXON:
					Taxon taxonValueToRead = (Taxon) ois.readObject();
					T = taxonValueToRead;
					break;
				default:
					break;
			}



		}
		catch(IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(1);

		}

		return T;
	}


	/** Read the object from Base64 string. */
	public static Object fromString( String s ) throws IOException ,
			ClassNotFoundException {
		byte [] data = Base64.getDecoder().decode( s );
		ObjectInputStream ois = new ObjectInputStream(
				new ByteArrayInputStream(  data ) );
		Object o  = ois.readObject();
		ois.close();
		return o;
	}

	/** Write the object to a Base64 string. */
	public static String toString( Serializable o ) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream( baos );
		oos.writeObject( o );
		oos.close();
		return Base64.getEncoder().encodeToString(baos.toByteArray());
	}

}
