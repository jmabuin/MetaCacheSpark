package com.github.jmabuin.metacachespark;

public class EnumModes {

    public enum Mode { HELP, QUERY, BUILD, ADD, INFO, ANNOTATE}

    public enum DatabaseType {HASHMAP, HASHMULTIMAP_GUAVA, HASHMULTIMAP_NATIVE, PARQUET, COMBINE_BY_KEY}

    public enum pairing_mode { none, files, sequences} // Pairing of queries

    public enum align_mode {none, semi_global} // Alignment mode

    public enum map_view_mode { none, mapped_only, all} // How to show mapping

    public enum taxon_print_mode { name_only, id_only, id_name} // how taxon formatting will be done

    public enum InputFormat {FASTA, FASTQ}


}
