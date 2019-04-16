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

package com.github.jmabuin.metacachespark.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Base class to read a sequence file (FASTA or FASTQ) sequentially from HDFS.
 * @author Jose M. Abuin
 */


public class SequenceReader {

    private static final Log LOG = LogFactory.getLog(SequenceReader.class); // LOG to show messages


    /**
     * From original MetaCache
     */
    private static String[] accession_prefix =
            {
                    "GCF_",
                    "AC_",
                    "NC_", "NG_", "NS_", "NT_", "NW_", "NZ_",
                    // "AEMK",
                    // "CCMK",
                    // "FPKY",
                    "MKHE",
                    "AE", "AJ", "AL", "AM", "AP", "AY",
                    "BA", "BK", "BX",
                    "CC", "CM", "CP", "CR", "CT", "CU",
                    "FM", "FN", "FO", "FP", "FQ", "FR",
                    "HE",
                    "JH"
            };

    /**
     * From original MetaCache
     */
    public static String extract_sequence_id(String text) {
        String sequid = extract_ncbi_accession_version_number(text);
        if(!sequid.isEmpty()) {
            return sequid;
        }

        sequid = extract_genbank_identifier(text);
        if(!sequid.isEmpty()) {
            return sequid;
        }

        return extract_ncbi_accession_number(text);
    }

    /**
     * From original MetaCache
     */
    public static String extract_ncbi_accession_number(String text) {

        if(text.isEmpty()) {
            return "";
        }

        for(String prefix : accession_prefix) {
            String num = extract_ncbi_accession_number(prefix, text);

            if(!num.isEmpty()) {
                return num;
            }

        }
        return "";
    }

    /**
     * From original MetaCache
     */
    public static String extract_ncbi_accession_number(String prefix, String text) {

        /** New Version
         if(text.isEmpty()) return "";

         int i = text.indexOf(prefix);
         if(i != -1) {
         int j = i + prefix.length();
         int k = end_of_accession_number(text,j);
         return text.substring(i, k-i);
         }
         return "";
         **/
        /** Previous version **/
        if(text.contains(prefix)) {
            int i = text.indexOf(prefix);
            int j = i + prefix.length();

            int k = text.indexOf("|", j);

            if(k == -1){
                k = text.indexOf(" ", j);
                if(k == -1) {
                    k = text.indexOf(".", j);
                    if (k == -1) {
                        k = text.indexOf("-", j);
                        if (k == -1) {
                            k = text.indexOf("_", j);
                            if (k == -1) {
                                k = text.indexOf(",", j);
                                if (k == -1) {
                                    k = text.length();
                                }

                            }
                        }
                    }
                }
            }

            return text.substring(i, k);

        }

        return "";

    }

    /**
     * From original MetaCache
     */
    public static String extract_ncbi_accession_version_number(String prefix, String text) {

        /** New version
         if(text.isEmpty()) {
         return "";
         }

         int i = text.indexOf(prefix);
         if(i < 20) {
         //find separator *after* prefix
         int s = text.indexOf('.', i+1);

         if(s == -1 || (s-i) > 20) {
         return "";
         }

         int k = end_of_accession_number(text,s+1);
         return text.substring(i, k-i);
         }
         return "";
         */
        /** Previous version **/

        int i = text.indexOf(prefix);

        if(i != -1) {
            // find version separator
            int j = text.indexOf(".", i+prefix.length());

            if(j == -1) {
                return "";
            }

            //find end of accession.version string
            int k = text.indexOf("|", j);

            if(k == -1){
                k = text.indexOf(" ", j);
                if(k == -1) {
                    k = text.indexOf("-", j);
                    if (k == -1) {
                        k = text.indexOf("_", j);
                        if (k == -1) {
                            k = text.indexOf(",", j);
                            if (k == -1) {
                                k = text.length();
                            }

                        }
                    }
                }
            }
            //System.err.println("[JMAbuin] i is "+i+" and k is "+k);
            return text.substring(i, k);

        }

        return "";

    }

    /**
     * From original MetaCache
     */
    public static String extract_ncbi_accession_version_number(String text) {

        /** New version
         if(text.isEmpty()) {
         return "";
         }

         //remove leading dots
         while(!text.isEmpty() && text.startsWith(".")) {
         text = text.substring(1, text.length());
         //text.rem.erase(0);
         }

         //try to find any known prefix + separator
         for(String prefix : accession_prefix) {
         String num = extract_ncbi_accession_version_number(prefix, text);
         if(!num.isEmpty()) {
         return num;
         }
         }

         //try to find version speparator
         int s = text.indexOf('.');
         if(s < 20) return text.substring(0, end_of_accession_number(text,s+1));

         return "";
         **/
        /** Previous version **/

        for(String prefix : accession_prefix) {
            String num = extract_ncbi_accession_version_number(prefix, text);
            if(!num.isEmpty()) return num;
        }

        return "";

    }

    /**
     * From original MetaCache
     */
    public static String extract_genbank_identifier(String text) {

        int i = text.indexOf("gi|");

        if(i != -1) {
            //skip prefix
            i += 3;

            //find end of number
            int j = text.indexOf('|', i);

            if(j == -1) {
                j = text.indexOf(' ', i);

                if(j == -1) {
                    j = text.length();
                }
            }

            return text.substring(i, j);
        }

        return "";
    }

    /**
     * From original MetaCache
     */
    public static Long extract_taxon_id(String text) {
        int i = text.indexOf("taxid");

        if(i != -1) {
            //skip "taxid" + separator char
            i += 6;
            //find end of number
            int j = text.indexOf('|', i);

            if(j == -1) {
                j = text.indexOf(' ', i);
                if(j == -1) {
                    j = text.length();
                }
            }

            try {
                return Long.parseLong(text.substring(i, j));
            }
            catch(Exception e) {
                return 0L;
            }
        }
        return 0L;
    }


    public static int end_of_accession_number(String text, int start) {
        if(start >= text.length()) {
            return text.length();
        }

        int k = text.indexOf('|', start);
        if(k != -1) return k;

        k = text.indexOf(' ', start);
        if(k != -1) return k;

        k = text.indexOf('-', start);
        if(k != -1) return k;

        k = text.indexOf('_', start);
        if(k != -1) return k;

        k = text.indexOf(',', start);
        if(k != -1) return k;

        return text.length();
    }


}
