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

public class EnumModes {

    public enum Mode { HELP, QUERY, BUILD, ADD, INFO, ANNOTATE}

    public enum QueryMode { PRECISE, THRESHOLD, FAST, VERY_FAST}

    public enum DatabaseType {HASHMAP, HASHMULTIMAP_GUAVA, HASHMULTIMAP_NATIVE, PARQUET, COMBINE_BY_KEY}

    public enum pairing_mode { none, files, sequences} // Pairing of queries

    public enum align_mode {none, semi_global} // Alignment mode

    public enum map_view_mode { none, mapped_only, all} // How to show mapping

    public enum taxon_print_mode { name_only, id_only, id_name} // how taxon formatting will be done

    public enum InputFormat {FASTA, FASTQ}


}
