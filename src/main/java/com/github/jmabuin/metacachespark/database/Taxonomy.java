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

package com.github.jmabuin.metacachespark.database;
import com.github.jmabuin.metacachespark.options.MetaCacheOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

/**
 * Class that represents a Taxonomy
 * @author Jose M. Abuin
 */
public class Taxonomy implements Serializable {

    private static final Log LOG = LogFactory.getLog(Taxonomy.class);

    private HashMap<Long, Taxon> 	taxa_;			// This is where the Taxonomy items reside. <ID, Taxon>
    private Taxon 					noTaxon_;		// Used to represent that there are no Taxons in the Taxonomy


    /**
     * Builder. It initializes the HashMap
     */
    public Taxonomy() {
        this.taxa_ = new HashMap<Long, Taxon>();
        this.noTaxon_ = new Taxon(0, 0, "--", Rank.none);
    }

    /**
     * Enum type that represents a Rank
     */
    public enum Rank {
        Sequence,
        Form,
        Variety,
        subSpecies,
        Species,
        subGenus,
        Genus,
        subTribe,
        Tribe,
        subFamily,
        Family,
        subOrder,
        Order,
        subClass,
        Class,
        subPhylum,
        Phylum,
        subKingdom,
        Kingdom,
        Domain,
        root,
        none;

        private static Rank[] vals = values();

        /**
         * Next method, that returns the next Rank in the correct order.
         * @return The next corresponding Rank
         */
        public Rank next()
        {
            return vals[(this.ordinal()+1) % vals.length];
        }

        /**
         * Previous method, that returns the previous Rank in the correct order.
         * @return The previous corresponding Rank
         */
        public Rank previous() {
            if(this.ordinal() == 0) {
                return vals[vals.length-1];
            }
            else {
                return vals[this.ordinal()-1];
            }

        }

    }

    // Variable to store the number of total Ranks
    public static int num_ranks = Rank.none.ordinal(); // +1?

    /**
     * This function gets the Rank that follows the one given as argument
     * @param r The Rank from we want to know the next one
     * @return The corresponding next Rank
     */
    public static Rank next_main_rank(Rank r) {
        switch(r) {
            case Sequence:     	return Rank.Species;
            case Form:         	return Rank.Species;
            case Variety:      	return Rank.Species;
            case subSpecies:   	return Rank.Species;
            case Species:      	return Rank.Genus;
            case subGenus:     	return Rank.Genus;
            case Genus:        	return Rank.Family;
            case subTribe:     	return Rank.Family;
            case Tribe:        	return Rank.Family;
            case subFamily:    	return Rank.Family;
            case Family:       	return Rank.Order;
            case subOrder:     	return Rank.Order;
            case Order:        	return Rank.Class;
            case subClass:     	return Rank.Class;
            case Class:        	return Rank.Phylum;
            case subPhylum:    	return Rank.Phylum;
            case Phylum:       	return Rank.Kingdom;
            case subKingdom:   	return Rank.Kingdom;
            case Kingdom:      	return Rank.Domain;
            case Domain:       	return Rank.root;
            default:
            case root:
            case none:			return Rank.none;
        }
    }

    /**
     * This function gets the previous Rank to the one given as argument
     * @param r The Rank we want to know the previous one
     * @return The previous Rank
     */
    public Rank prev_main_rank(Rank r) {
        switch(r) {
            case Sequence:     return Rank.none;
            case Form:         return Rank.Sequence;
            case Variety:      return Rank.Sequence;
            case subSpecies:   return Rank.Sequence;
            case Species:      return Rank.Sequence;
            case subGenus:     return Rank.Species;
            case Genus:        return Rank.Species;
            case subTribe:     return Rank.Genus;
            case Tribe:        return Rank.Genus;
            case subFamily:    return Rank.Genus;
            case Family:       return Rank.Genus;
            case subOrder:     return Rank.Family;
            case Order:        return Rank.Family;
            case subClass:     return Rank.Order;
            case Class:        return Rank.Order;
            case subPhylum:    return Rank.Class;
            case Phylum:       return Rank.Class;
            case subKingdom:   return Rank.Phylum;
            case Kingdom:      return Rank.Phylum;
            case Domain:       return Rank.Kingdom;
            case root:         return Rank.Domain;
            default:
            case none: return Rank.none;
        }
    }

    //---------------------------------------------------------------
    public Rank next(Rank r) {
        return r.ordinal() < num_ranks ? (Rank.values()[r.ordinal() + 1]) : r;
    }
    public Rank previous(Rank r) {
        return r.ordinal() > 0 ? (Rank.values()[r.ordinal() - 1]) : r;
    }

    /**
     * Function that, given a Rank name, returns the corresponding Rank
     * @param name A String representing the Rank name
     * @return The corresponding Rank
     */
    public static Rank rank_from_name(String name) {
        /*
        if(name == "sequence")         return rank::Sequence;
        if(name == "genome")           return rank::Sequence;
        if(name == "form")             return rank::Form;
        if(name == "forma")            return rank::Form;
        if(name == "variety")          return rank::Variety;
        if(name == "varietas")         return rank::Variety;
        if(name == "subspecies")       return rank::subSpecies;
        if(name == "species")          return rank::Species;
        if(name == "species group")    return rank::subGenus;
        if(name == "species subgroup") return rank::subGenus;
        if(name == "subgenus")         return rank::subGenus;
        if(name == "genus")            return rank::Genus;
        if(name == "subtribe")         return rank::subTribe;
        if(name == "tribe")            return rank::Tribe;
        if(name == "subfamily")        return rank::subFamily;
        if(name == "family")           return rank::Family;
        if(name == "superfamily")      return rank::subOrder;
        if(name == "parvorder")        return rank::subOrder;
        if(name == "infraorder")       return rank::subOrder;
        if(name == "suborder")         return rank::subOrder;
        if(name == "order")            return rank::Order;
        if(name == "superorder")       return rank::subClass;
        if(name == "infraclass")       return rank::subClass;
        if(name == "subclass")         return rank::subClass;
        if(name == "class")            return rank::Class;
        if(name == "superclass")       return rank::subPhylum;
        if(name == "subphylum")        return rank::subPhylum;
        if(name == "phylum")           return rank::Phylum;
        if(name == "division")         return rank::Phylum;
        if(name == "superphylum")      return rank::subKingdom;
        if(name == "subkingdom")       return rank::subKingdom;
        if(name == "kingdom")          return rank::Kingdom;
        if(name == "subdomain")        return rank::Kingdom;
        if(name == "superkingdom")     return rank::Domain;
        if(name == "domain")           return rank::Domain;
        if(name == "root")             return rank::root;
        return rank::none;
         */

        String lower_name = name.toLowerCase();
        //Rank return_rank = Rank.none;

        switch (lower_name) {
            case "sequence":    return Rank.Sequence;
            case "genome":        return Rank.Sequence;
            case "form":         return Rank.Form;
            case "forma":         return Rank.Form;
            case "variety":       return Rank.Variety;
            case "varietas":      return Rank.Variety;
            case "subspecies":    return Rank.subSpecies;
            case "species":       return Rank.Species;
            case "species group": return Rank.subGenus;
            case "species subgroup": return Rank.subGenus;
            case "subgenus":      return Rank.subGenus;
            case "genus":        return Rank.Genus;
            case "subtribe":      return Rank.subTribe;
            case "tribe":        return Rank.Tribe;
            case "subfamily":     return Rank.subFamily;
            case "family":       return Rank.Family;
            case "superfamily":  return Rank.subOrder;
            case "parvorder":        return Rank.subOrder;
            case "infraorder":       return Rank.subOrder;
            case "suborder":      return Rank.subOrder;
            case "order":        return Rank.Order;
            case "superorder":       return Rank.subClass;
            case "infraclass":       return Rank.subClass;
            case "subclass":      return Rank.subClass;
            case "class":         return Rank.Class;
            case "superclass":       return Rank.subPhylum;
            case "subphylum":     return Rank.subPhylum;
            case "phylum":      return Rank.Phylum;
            case "division":      return Rank.Phylum;
            case "superphylum":      return Rank.subKingdom;
            case "subkingdom":    return Rank.subKingdom;
            case "kingdom":       return Rank.Kingdom;
            case "subdomain":        return Rank.Kingdom;
            case "superkingdom":  return Rank.Domain;
            case "domain":        return Rank.Domain;
            case "root":         return Rank.root;
            default: return Rank.none;
        }

/*
        if(name.equals("sequence"))      return Rank.Sequence;
        if(name.equals("genome"))        return Rank.Sequence;
        if(name.equals("form"))          return Rank.Form;
        if(name.equals("forma"))         return Rank.Form;
        if(name.equals("variety"))       return Rank.Variety;
        if(name.equals("varietas"))      return Rank.Variety;
        if(name.equals("subspecies"))    return Rank.subSpecies;
        if(name.equals("species"))       return Rank.Species;
        if(name.equals("species group")) return Rank.subGenus;
        if(name.equals("species subgroup")) return Rank.subGenus;
        if(name.equals("subgenus"))      return Rank.subGenus;
        if(name.equals("genus"))         return Rank.Genus;
        if(name.equals("subtribe"))      return Rank.subTribe;
        if(name.equals("tribe"))         return Rank.Tribe;
        if(name.equals("subfamily"))     return Rank.subFamily;
        if(name.equals("family"))        return Rank.Family;
        if(name.equals("superfamily") )  return Rank.subOrder;
        if(name.equals("parvorder"))        return Rank.subOrder;
        if(name.equals("infraorder"))       return Rank.subOrder;
        if(name.equals("suborder"))      return Rank.subOrder;
        if(name.equals("order"))         return Rank.Order;
        if(name.equals("superorder"))       return Rank.subClass;
        if(name.equals("infraclass"))       return Rank.subClass;
        if(name.equals("subclass"))      return Rank.subClass;
        if(name.equals("class"))         return Rank.Class;
        if(name.equals("superclass"))       return Rank.subPhylum;
        if(name.equals("subphylum"))     return Rank.subPhylum;
        if(name.equals("phylum"))        return Rank.Phylum;
        if(name.equals("division"))      return Rank.Phylum;
        if(name.equals("superphylum"))      return Rank.subKingdom;
        if(name.equals("subkingdom"))    return Rank.subKingdom;
        if(name.equals("kingdom"))       return Rank.Kingdom;
        if(name.equals("subdomain"))        return Rank.Kingdom;
        if(name.equals("superkingdom"))  return Rank.Domain;
        if(name.equals("domain"))        return Rank.Domain;
        if(name.equals("root"))          return Rank.root;
        return Rank.none;*/
    }

    /**
     * Function that, given a Rank, returns a String representing its name
     * @param r The Rank
     * @return The Rank name
     */
    public static String rank_name(Rank r) {

        return r.name();

    }

    /**
     * Function that returns the HashMap of <ID, Taxon>
     * @return The HashMap containing the pairs ID, Taxon
     */
    public HashMap<Long, Taxon> getTaxa_() {
        return this.taxa_;
    }

    /**
     * Function that checks if our HashMap is empty
     * @return True if the HashMap is empty, False otherwise
     */
    public boolean empty() {
        return taxa_.isEmpty();
    }

    /**
     * Seter for the HasMap of Taxons
     * @param taxa_ The new HashMap
     */
    public void setTaxa_(HashMap<Long, Taxon> taxa_) {
        this.taxa_ = taxa_;
    }

    /**
     * Getter fot the noTaxon_ variable
     * @return A Taxon representing the noTaxon_ variable
     */
    public Taxon getNoTaxon_() {
        return noTaxon_;
    }

    /**
     * Setter for the noTaxon_ variable
     * @param noTaxon_ The new noTaxon_
     */
    public void setNoTaxon_(Taxon noTaxon_) {
        this.noTaxon_ = noTaxon_;
    }

    /**
     * Function that returns the number of Ranks
     * @return the number of Ranks
     */
    public int getNum_ranks() {
        return num_ranks;
    }

    /**
     * Setter for the number of Ranks. This function shouldn't exist
     * @param num_ranks The new number of Ranks
     */
    public void setNum_ranks(int num_ranks) {
        this.num_ranks = num_ranks;
    }


    /**
     * Function that returns the lineage from the Taxon passed as argument to its parents
     * @param tax The Taxon we want to know the lineage
     * @return An ArrayList containing the IDs of the Taxons that compose the Taxon lineage.
     */
    public ArrayList<Long> lineage(Taxon tax) {
        return this.lineage(tax.getTaxonId());
    }

    /**
     * Function that returns the lineage from the Taxon passed as argument to its parents
     * @param id A Long variable that represents the Taxon id from the Taxon we want to know the lineage
     * @return An ArrayList containing the IDs of the Taxons that compose the Taxon lineage.
     */
    public ArrayList<Long> lineage(Long id) {

        ArrayList<Long> lin = new ArrayList<Long>();
        Taxon currentTaxon = null;

        while(id != 0) {

            currentTaxon = this.taxa_.get(id);
            if (currentTaxon != null) {

                lin.add(id);
                if(currentTaxon.getParentId() != id) {
                    id = currentTaxon.getParentId();
                }
                else {
                    id = (long)0;
                }

            }
            else {
                id = (long) 0;
            }

        }

        //LOG.warn("[JMAbuin] lineage size: "+lin.size());

        return lin;
    }

    /**
     * This function acts equals to the lineage function, but it returns an Array instead of an ArrayList
     * @param id The Taxon id
     * @return An Array containing the IDs of the Taxons that compose the Taxon lineage.
     */
    public Long[] ranks(Long id) {

        Long[] lin = new Long[Rank.none.ordinal()];

        for(int i = 0; i< lin.length; i++) {
            lin[i] =(long) 0;
        }

        Taxon currentTaxon = null;

        while(id != (long)0) {
            //LOG.warn("JMABUIN "+id);
            currentTaxon = this.taxa_.get(id);



            if(currentTaxon != null) {
                //LOG.warn("JMABUIN "+currentTaxon.getTaxonName() + currentTaxon.getRank().name());
                //LOG.warn("JMABUIN "+currentTaxon.getRank().ordinal() +" - "+Rank.none.ordinal());
                if((currentTaxon.getRank().ordinal() != Rank.none.ordinal())) { // && (lin[it.getRank().ordinal()] == 0)
                    lin[currentTaxon.getRank().ordinal()] = currentTaxon.getTaxonId();
                }
                if(currentTaxon.getParentId() != id){
                    id = currentTaxon.getParentId();
                }
                else {
                    id = (long)0;
                }

            }
            else {
                id = (long) 0;
            }

        }
        //LOG.warn("\n\n");
        return lin;

    }

    /**
     * This function acts equals to the lineage function, but it returns an Array instead of an ArrayList
     * @param tax The Taxon
     * @return An Array containing the IDs of the Taxons that compose the Taxon lineage.
     */
    public Long[] ranks(Taxon tax) {
        return this.ranks((Long)tax.getTaxonId());
    }

    /**
     * Function that adds a new pair of <ID, Taxon> to the HashMap
     * @param taxonId the Taxon ID
     * @param parentId The new Taxon parent ID
     * @param taxonName The Taxon name
     * @param rankName The Taxon Rank name
     */
    public void emplace(long taxonId, long parentId, String taxonName, String rankName) {
        //LOG.warn("Emplacing: "+taxonId+" "+rankName+" :" + Taxonomy.rank_from_name(rankName.toLowerCase()).name() + " : "+Taxonomy.rank_from_name(rankName.toLowerCase()).ordinal());
        this.emplace(taxonId, parentId, taxonName, Taxonomy.rank_from_name(rankName.toLowerCase()) );

    }

    /**
     * Function that adds a new pair of <ID, Taxon> to the HashMap
     * @param taxonId the Taxon ID
     * @param parentId The new Taxon parent ID
     * @param taxonName The Taxon name
     * @param rank The Taxon Rank
     */
    public void emplace(long taxonId, long parentId, String taxonName, Rank rank) {
        //LOG.warn("Emplacing: "+taxonId+" "+parentId+" :" + taxonName + " : "+rank.name() + " : " +rank.ordinal());
        this.taxa_.put(taxonId,new Taxon(taxonId, parentId, taxonName, rank));

    }

    /**
     * Function that returns the number of items in the HashMap
     * @return The number of items in the HashMap
     */
    public long taxon_count() {
        return taxa_.size();
    }

    /**
     * Function that tries to rank all unranked Taxons in the Taxonomy
     */
    public void rank_all_unranked() {
        for(Taxon tax : taxa_.values()) {

            if(tax.getRank().ordinal() == Rank.none.ordinal()) {

                Rank lr = this.lowest_rank(tax);

                if(lr.ordinal() != Rank.none.ordinal()) {
                    if (lr.ordinal() > Rank.subSpecies.ordinal()) {
                        lr = lr.previous();
                    }
                    tax.setRank(lr);

                }
            }
        }
    }

    /**
     * Function that gets the most closer parent that is ranked
     * @param id The Taxon id
     * @return The most closer parent ranked
     */
    public Rank	lowest_rank(long id) {

        Taxon currentTaxon = null;

        while(id != (long)0) {

            // Gets the Taxon corresponding to the ID
            currentTaxon = this.taxa_.get(id);

            // If the Taxon exists in the HashMap
            if(currentTaxon != null) {

                // If the Taxon is not the none Taxon, returns
                if(currentTaxon.getRank().ordinal() != Rank.none.ordinal()) {
                    return currentTaxon.getRank();
                }

                // If the Taxon has a parent Id, we look in the father
                if(currentTaxon.getParentId() != id){
                    //System.err.println("Current taxon id: "+id+ " and parent is "+currentTaxon.getParentId());
                    id = currentTaxon.getParentId();
                }
                // Otherwise we can not get the lowest rank and id is 0 to break the loop
                else {
                    id = (long) 0;
                }

            }
            // Otherwise we can not get the lowest rank and id is 0 to break the loop
            else {
                id = (long) 0;
            }

        }

        // If we got here, we have to return the none Rank
        return Rank.none;

    }

    /**
     * Function that gets the most closer parent that is ranked
     * @param tax The Taxon
     * @return The most closer parent ranked
     */
    public Rank	lowest_rank(Taxon tax) {
        return lowest_rank(tax.getTaxonId());
    }

    /**
     * Checks if two lineages contains the same taxon ID
     * @param lina ArrayList representing IDs from lineage A
     * @param linb ArrayList representing IDs from lineage B
     * @return A long value that corresponds with the ID of the Taxons if the same ID exists in the two lineages. 0 otherwise
     */
    public static long lca_id(ArrayList<Long> lina,ArrayList<Long> linb) {
        for(long ta : lina) {
            for(long tb : linb) {
                if(ta == tb) return ta;
            }
        }

        return 0;
    }

    /**
     * Funtion that returns a Taxon given an ID
     * @param id The ID we are looking for
     * @return The Taxon if it exists, noTaxon_ otherwise
     */
    public Taxon pos (long id) {
        if(id < 1) return noTaxon_;
        Taxon it = taxa_.get(id);
        return (it != null) ? it : noTaxon_;

    }

    /**
     * Function that returns the Taxon that belongs to two lineages
     * @param lina An ArrayList of Long items representing lineage A
     * @param linb An ArrayList of Long items representing lineage B
     * @return	The Taxon
     */
    public Taxon lca(ArrayList<Long> lina, ArrayList<Long> linb) {
        return this.pos(lca_id(lina,linb));
    }

    /**
     * Function that returns the Taxon that belongs to two lineages
     * @param lina An Array of long items representing lineage A
     * @param linb An Array of long items representing lineage B
     * @return	The Taxon
     */
    public Taxon lca(long lina[], long linb[]) {
        //LOG.warn("JMABUIN : Taxon.lca");
        //LOG.warn("JMABUIN : Taxon.lca :: " + ranked_lca_id(lina,linb)+" -- "+this.pos(ranked_lca_id(lina,linb)).getTaxonName());
        return this.pos(ranked_lca_id(lina,linb));
    }

    /**
     * Function that checks if two lineages contains a similar Ranked Taxon
     * @param lina An Array of Ranks
     * @param linb An Array of Ranks
     * @return The Taxon ID that is present in the two arrays or 0 if there is no an equal Taxon or one of them is unranked
     */
    public static long ranked_lca_id( long lina[],long linb[]) {

        if(lina == null || linb == null || lina.length == 0 || linb.length == 0) {
            //LOG.warn("Null ou cero");
            return 0;
        }

        for(int i = 0; i < Rank.root.ordinal(); ++i) {
            //LOG.warn("Un: "+lina[i]+". Dous: "+linb[i]);
            if((lina[i] > 0) && (lina[i] == linb[i])) return lina[i];
        }

        return 0;
    }

    /**
     * Function that returns a common Ranked ancestor of two Taxons
     * @param a The Taxon a
     * @param b The Taxon b
     * @return The common ranked ancestor
     */
    public Taxon ranked_lca(Taxon a, Taxon b) {
        return ranked_lca(a.getTaxonId(), b.getTaxonId());
    }



    /**
     * Function that, given two Taxon IDs, obtains its Ranks and checks if they have a common ancestor
     * @param a ID of Taxon a
     * @param b ID of Taxon b
     * @return The common ranked ancestor
     */
    public Taxon ranked_lca(long a, long b) {
        return this.pos(ranked_lca_id(ranks(a), ranks(b) ));
    }

    public Taxon ranked_lca_map(long a, long b) {
        return this.pos(ranked_lca_id(ranks(a), ranks(b) ));
    }

    /**
     * Function that returns the Ranks of the ancestors of a current Taxon ID
     * @param id The Taxon ID
     * @return An Array containing the Ranks of the ancestors
     */
    public long[] ranks(long id) {

        long[] lin = new long[this.getNum_ranks()];


        for(long x : lin) {
            x = 0;
        }

        while(id != 0) {

            Taxon it = taxa_.get(id);

            if(it != null) {
                if((it.getRank().ordinal() != Rank.none.ordinal())) {
                    lin[it.getRank().ordinal()] = it.getTaxonId();
                }

                if(it.getParentId() != id) {
                    id = it.getParentId();
                } else {
                    id = 0;
                }
            } else {
                id = 0;
            }
        }

        return lin;
    }

    /*
    public ArrayList<Long> lineage(Long id) {

        ArrayList<Long> lin = new ArrayList<Long>();
        Taxon currentTaxon = null;

        while(id != 0) {

            currentTaxon = this.taxa_.get(id);
            if (currentTaxon != null) {

                lin.add(id);
                if(currentTaxon.getParentId() != id) {
                    id = currentTaxon.getParentId();
                }
                else {
                    id = (long)0;
                }

            }
            else {
                id = (long) 0;
            }

        }

        //LOG.warn("[JMAbuin] lineage size: "+lin.size());

        return lin;
    }
     */

/*
    public HashMap<String, Long> ranks_map(long id) {

        long[] lin = new long[this.getNum_ranks()];


        for(long x : lin) {
            x = 0;
        }

        while(id != 0) {

            Taxon it = taxa_.get(id);

            if(it != null) {
                if(it.getRank().ordinal() != Rank.none.ordinal()) {
                    lin[it.getRank().ordinal()] = it.getTaxonId();
                }

                if(it.getParentId() != id) {
                    id = it.getParentId();
                } else {
                    id = 0;
                }
            } else {
                id = 0;
            }
        }

        return lin;
    }
*/

    public void write(String fileName, JavaSparkContext jsc) {

        // Try to open the filesystem (HDFS) and sequence file
        try {
            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(fileName));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

            // Write data
            bw.write(String.valueOf(this.getTaxa_().size()));
            bw.newLine();

            StringBuffer currentLine = new StringBuffer();

            for(Map.Entry<Long, Taxon> currentEntry: this.getTaxa_().entrySet()) {

                currentLine.append(currentEntry.getKey());
                currentLine.append(":");

                currentLine.append(currentEntry.getValue().getTaxonId());
                currentLine.append(":");
                currentLine.append(currentEntry.getValue().getParentId());
                currentLine.append(":");
                currentLine.append(currentEntry.getValue().getTaxonName());
                currentLine.append(":");
                currentLine.append(currentEntry.getValue().getRank().name());
                bw.write(currentLine.toString());
                bw.newLine();

                currentLine.delete(0, currentLine.toString().length());

            }

            bw.close();
            outputStream.close();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ fileName+ " because of IO error in Taxonomy.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ fileName+ " because of IO error in Taxonomy.");
            e.printStackTrace();
            //System.exit(1);
        }

    }


    public void read(String fileName, JavaSparkContext jsc) {

        // Try to open the filesystem (HDFS) and sequence file
        try {
            FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(fileName));

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

            // read data
            long numberItems = Long.parseLong(br.readLine());

            String currentLine;

            while((currentLine = br.readLine()) != null) {

                String parts[] = currentLine.split(":");

                if (parts.length ==5) {
                    this.taxa_.put(Long.parseLong(parts[0]), new Taxon(Long.parseLong(parts[1]), Long.parseLong(parts[2])
                            , parts[3], Taxonomy.rank_from_name(parts[4].toLowerCase())));
                }
                else {
                    Long index = Long.parseLong(parts[0]);
                    Long id = Long.parseLong(parts[1]);
                    Long parent_id = Long.parseLong(parts[2]);

                    StringBuilder name = new StringBuilder();

                    for (int i = 3; i< parts.length -2; ++i ) {
                        name.append(parts[i]);
                        if (i < parts.length -3) {
                            name.append(":");
                        }

                    }


                    Taxonomy.Rank rank = Taxonomy.rank_from_name(parts[parts.length -1 ].toLowerCase());

                    this.taxa_.put(index, new Taxon(id, parent_id
                            , name.toString(), rank));
                }

            }


            br.close();
            inputStream.close();

        }
        catch (IOException e) {
            LOG.error("Could not write file "+ fileName+ " because of IO error in Taxonomy.");
            e.printStackTrace();
            //System.exit(1);
        }
        catch (Exception e) {
            LOG.error("Could not write file "+ fileName+ " because of IO error in Taxonomy.");
            e.printStackTrace();
            //System.exit(1);
        }

    }

    public void clear() {
        this.taxa_.clear();
    }


    public Long ancestor(Long id, int rank) {

        return this.lineage(id).get(rank);

    }

    public Long ancestor(Long id, Rank rank) {

        //Long[] ranks = this.ranks(id);

        //return ranks[rank.ordinal()];

        return this.lineage_hash(id, rank);

    }


    public Long lineage_hash(Long id, Rank rank) {

        List<Long> lin = this.lineage(id);

        //Rank[] vals = Rank.values();

        HashMap<String, Long> values_hashmap = new HashMap<>();

        for(int i = 0; i< lin.size(); ++i) {
            values_hashmap.put(this.taxa_.get(lin.get(i)).rank_name(), lin.get(i));
        }

        return values_hashmap.get(rank.name());
    }
}
