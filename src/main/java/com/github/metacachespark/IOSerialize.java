package com.github.metacachespark;

import java.io.*;

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


}
