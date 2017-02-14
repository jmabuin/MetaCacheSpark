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

package com.github.jmabuin.metacachespark.database;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by chema on 1/13/17.
 */
public class Taxonomy implements Serializable {


	//private ArrayList<Taxon> taxa_;
	private HashMap<Long, Taxon> taxa_;
	private Taxon noTaxon_;

	/*
	public Taxonomy() {
		this.taxa_ = new ArrayList<Taxon>();
	}
	*/

	public Taxonomy() {
		this.taxa_ = new HashMap<Long, Taxon>();
	}

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

		public Rank next()
		{
			return vals[(this.ordinal()+1) % vals.length];
		}

		public Rank previous() {
			if(this.ordinal() == 0) {
				return vals[vals.length-1];
			}
			else {
				return vals[this.ordinal()-1];
			}


		}

	}


	public static int num_ranks = Rank.none.ordinal(); // +1?

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

	//---------------------------------------------------------------
	public static Rank rank_from_name(String name) {
		if(name.equals("sequence"))      return Rank.Sequence;
		if(name.equals("genome"))        return Rank.Sequence;
		if(name.equals("form"))          return Rank.Form;
		if(name.equals("forma"))         return Rank.Form;
		if(name.equals("variety"))       return Rank.Variety;
		if(name.equals("varietas"))      return Rank.Variety;
		if(name.equals("subspecies"))    return Rank.subSpecies;
		if(name.equals("species"))       return Rank.Species;
		if(name.equals("subgenus"))      return Rank.subGenus;
		if(name.equals("genus"))         return Rank.Genus;
		if(name.equals("subtribe"))      return Rank.subTribe;
		if(name.equals("tribe"))         return Rank.Tribe;
		if(name.equals("subfamily"))     return Rank.subFamily;
		if(name.equals("family"))        return Rank.Family;
		if(name.equals("suborder"))      return Rank.subOrder;
		if(name.equals("order"))         return Rank.Order;
		if(name.equals("subclass"))      return Rank.subClass;
		if(name.equals("class"))         return Rank.Class;
		if(name.equals("subphylum"))     return Rank.subPhylum;
		if(name.equals("phylum"))        return Rank.Phylum;
		if(name.equals("division"))      return Rank.Phylum;
		if(name.equals("subkingdom"))    return Rank.subKingdom;
		if(name.equals("kingdom"))       return Rank.Kingdom;
		if(name.equals("superkingdom"))  return Rank.Domain;
		if(name.equals("domain"))        return Rank.Domain;
		if(name.equals("root"))          return Rank.root;
		return Rank.none;
	}


	//---------------------------------------------------------------
	public static String rank_name(Rank r) {

		return r.name();

	}
/*
	public ArrayList<Taxon> getTaxa_() {
		return taxa_;
	}
*/

	public HashMap<Long, Taxon> getTaxa_() {
		return this.taxa_;
	}

	public boolean empty() {
		return taxa_.isEmpty();
	}
/*
	public void setTaxa_(ArrayList<Taxon> taxa_) {
		this.taxa_ = taxa_;
	}
*/

	public void setTaxa_(HashMap<Long, Taxon> taxa_) {
		this.taxa_ = taxa_;
	}

	public Taxon getNoTaxon_() {
		return noTaxon_;
	}

	public void setNoTaxon_(Taxon noTaxon_) {
		this.noTaxon_ = noTaxon_;
	}

	public int getNum_ranks() {
		return num_ranks;
	}

	public void setNum_ranks(int num_ranks) {
		this.num_ranks = num_ranks;
	}

	public ArrayList<Long> lineage(Taxon tax) {
		return this.lineage(tax.getTaxonId());
	}

	public ArrayList<Long> lineage(Long id) {

		ArrayList<Long> lin = new ArrayList<Long>();
		Taxon currentTaxon = null;

		while(id != 0) {
			/*
			boolean foundTaxon = false;

			Iterator<Taxon> taxonIterator = this.taxa_.values().iterator();

			while(taxonIterator.hasNext()) {
				currentTaxon = taxonIterator.next();

				if(currentTaxon.getTaxonId() == id) {
					foundTaxon = true;
					break;
				}

			}
			*/
			currentTaxon = this.taxa_.get(id);
			if (currentTaxon != null) {

				lin.add(id);
				if(currentTaxon.getParentId() != 0) {
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

		return lin;
	}

	public Long[] ranks(Long id) {

		Long[] lin = new Long[Rank.none.ordinal()];

		for(int i = 0; i< lin.length; i++) {
			lin[i] =(long) 0;
		}

		Taxon currentTaxon = null;

		while(id != (long)0) {
/*
			boolean foundTaxon = false;

			Iterator<Taxon> taxonIterator = this.taxa_.iterator();

			while(taxonIterator.hasNext()) {
				currentTaxon = taxonIterator.next();

				if(currentTaxon.getTaxonId() == id) {
					foundTaxon = true;
					break;
				}

			}

			if (foundTaxon) {
*/
			currentTaxon = this.taxa_.get(id);
			if(currentTaxon != null) {
				if(currentTaxon.getRank() != Rank.none) {
					lin[currentTaxon.getRank().ordinal()] = currentTaxon.getTaxonId();
				}
				if(currentTaxon.getParentId() != 0){
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

		return lin;

	}

	public long[] ranks(Taxon tax) {
		return this.ranks(tax.getTaxonId());
	}

	public void emplace(long taxonId, long parentId, String taxonName, String rankName) {

		this.emplace(taxonId, parentId, taxonName, rank_from_name(rankName) );

	}

	public void emplace(long taxonId, long parentId, String taxonName, Rank rank) {

		this.taxa_.put(taxonId,new Taxon(taxonId, parentId, taxonName, rank));

	}

	public long taxon_count() {
		return taxa_.size();
	}

	public void rank_all_unranked() {
		for(Taxon tax : taxa_.values()) {

			if(tax.getRank() == Rank.none) {
				//System.err.println("Ranking unranked: "+tax.getTaxonId());
				Rank lr = this.lowest_rank(tax);
				if(lr != Rank.none) {
					if (lr.compareTo(Rank.subSpecies) > 0) {
						lr = lr.previous();
					}
					tax.setRank(lr);

				}
			}
		}
	}

	public Rank	lowest_rank(long id) {

		Taxon currentTaxon = null;
		//int currentTaxonIndex = -1;

		while(id != (long)0) {
			//System.err.println("Current taxon id: "+id);

			/*
			boolean foundTaxon = false;

			Iterator<Taxon> taxonIterator = this.taxa_.iterator();

			while(taxonIterator.hasNext()) {
				currentTaxon = taxonIterator.next();

				if(currentTaxon.getTaxonId() == id) {
					foundTaxon = true;
					break;
				}

			}
			*/

			//currentTaxonIndex = this.taxa_.indexOf(new Taxon(id));
			currentTaxon = this.taxa_.get(id);

			//if (foundTaxon) {
			if(currentTaxon != null) {
				//currentTaxon = this.taxa_.get(currentTaxonIndex);
				//System.err.println("Found taxon "+id);
				if(currentTaxon.getRank() != Rank.none) {
					return currentTaxon.getRank();
				}
				if(currentTaxon.getParentId() != id){
					//System.err.println("Current taxon id: "+id+ " and parent is "+currentTaxon.getParentId());
					id = currentTaxon.getParentId();
				}
				else {
					id = (long) 0;
				}

			}
			else {
				id = (long) 0;
			}

		}

		return Rank.none;

	}

	public Rank	lowest_rank(Taxon tax) {
		return lowest_rank(tax.getTaxonId());
	}

	public static long lca_id(ArrayList<Long> lina,ArrayList<Long> linb) {
		for(long ta : lina) {
			for(long tb : linb) {
				if(ta == tb) return ta;
			}
		}

		return 0;
	}

	public Taxon pos (long id) {
		if(id < 1) return noTaxon_;
		Taxon it = taxa_.get(id);
		return (it != null) ? it : noTaxon_;

	}

	public Taxon lca(ArrayList<Long> lina, ArrayList<Long> linb) {
		return this.pos(lca_id(lina,linb));
	}

	public Taxon lca(long lina[], long linb[]) {
		return this.pos(ranked_lca_id(lina,linb));
	}

	public static long ranked_lca_id( long lina[],long linb[]) {

		for(int i = 0; i < Rank.root.ordinal(); ++i) {
			if((lina[i] > 0) && (lina[i] == linb[i])) return lina[i];
		}

		return 0;
	}

	public Taxon ranked_lca(Taxon a, Taxon b) {
		return ranked_lca(a.getTaxonId(), b.getTaxonId());
	}

	public Taxon ranked_lca(long a, long b) {
		return this.pos(ranked_lca_id(ranks(a), ranks(b) ));
	}

	public long[] ranks(long id) {

		long[] lin = new long[this.getNum_ranks()];


		for(long x : lin) {
			x = 0;
		}

		while(id != 0) {

			Taxon it = taxa_.get(id);

			if(it != null) {
				if(it.getRank() != Rank.none) {
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
}
