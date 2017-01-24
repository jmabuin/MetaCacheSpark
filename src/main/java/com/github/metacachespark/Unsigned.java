package com.github.metacachespark;

import java.util.BitSet;
import org.apache.commons.lang.ArrayUtils;

/**
 * Created by chema on 1/16/17.
 */
public class Unsigned extends BitSet {

	public Unsigned() {
		super();
	}

	public Unsigned(int nbits) {
		super(nbits);
	}

	public Unsigned(String hexString) {

		super(hexString.length()*4);
		this.parseFromBinString(this.Hex2Bin(hexString));


	}

	// Equivalent to "Var << nbits"
	public void shiftLeft(int nbits) {

		if(nbits >= this.length()) {
			this.clear();
		}
		else {
			int i = 0;

			for (i = this.length() -1; i > nbits-1; i--) {
				this.set(i , this.get(i - nbits));
			}

			for(i = nbits-1; i >= 0; i--) {
				this.clear(i);
			}
		}
	}

	// Equivalent to "Var >> nbits"
	public void shiftRight(int nbits) {

		if(nbits >= this.length()) {
			this.clear();
		}
		else {
			int i = 0;

			for (i = 0; i <  this.length() - nbits; i++) {
				this.set(i , this.get(i + nbits));
			}
			for(i = this.length() - nbits; i < this.length(); i++) {
				this.clear(i);
			}
		}
	}

	public String Hex2Bin(String hex) {

		char[] characters = hex.toCharArray();

		String returningString = "";

		int i = 0;

		//for(i = characters.length-1; i>=0; i--) {
		for(i = 0; i<characters.length; i++) {
			switch(characters[i]) {
				case '0':
					returningString = returningString + "0000";
					break;
				case '1':
					returningString = returningString + "0001";
					break;
				case '2':
					returningString = returningString + "0010";
					break;
				case '3':
					returningString = returningString + "0011";
					break;
				case '4':
					returningString = returningString + "0100";
					break;
				case '5':
					returningString = returningString + "0101";
					break;
				case '6':
					returningString = returningString + "0110";
					break;
				case '7':
					returningString = returningString + "0111";
					break;
				case '8':
					returningString = returningString + "1000";
					break;
				case '9':
					returningString = returningString + "1001";
					break;
				case 'A':
					returningString = returningString + "1010";
					break;
				case 'B':
					returningString = returningString + "1011";
					break;
				case 'C':
					returningString = returningString + "1100";
					break;
				case 'D':
					returningString = returningString + "1101";
					break;
				case 'E':
					returningString = returningString + "1110";
					break;
				case 'F':
					returningString = returningString + "1111";
					break;
				case 'a':
					returningString = returningString + "1010";
					break;
				case 'b':
					returningString = returningString + "1011";
					break;
				case 'c':
					returningString = returningString + "1100";
					break;
				case 'd':
					returningString = returningString + "1101";
					break;
				case 'e':
					returningString = returningString + "1110";
					break;
				case 'f':
					returningString = returningString + "1111";
					break;
				default:
					break;
			}
			//System.out.println(returningString);
		}

		System.out.println("Hex2Bin:" +returningString);

		return returningString;
	}

	public void parseFromBinString(String binaryString) {

		char[] characters = binaryString.toCharArray();

		int i = 0;
		int j = characters.length -1;

		for(i = 0; i< characters.length; i++) {

			switch(characters[i]) {
				case '1':
					this.set(j);
					break;
				case '0':
					this.clear(j);
					break;
				default:
					break;
			}

			j--;

		}

	}

	public String toBinString() {

		int i = 0;

		String returningString = "";

		for(i = this.length() -1; i >= 0; i--) {

			if(this.get(i)) {
				returningString = returningString + "1";
			}
			else {
				returningString = returningString + "0";
			}


		}

		return returningString;

	}

}
