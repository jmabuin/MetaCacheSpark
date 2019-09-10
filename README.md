# What's MetaCacheSpark about? #

**MetaCacheSpark** is a tool that allows to use the metagenomics minhashing algorithm from [**metacache**][1] by means of a Big Data environment by using Apache Spark as engine.

If you use **MetaCacheSpark**, please cite this article:

> TBA

The original **metacache** article is:

> André Müller, Christian Hundt, Andreas Hildebrandt, Thomas Hankeln, Bertil Schmidt. ["MetaCache: context-aware classification of metagenomic reads using minhashing"][3]. Bioinformatics, Volume 33, Issue 23, 01 December 2017, Pages 3740–3748.

# Structure #
The project keeps a standard Maven structure. The source code is in the *src/main* folder. Inside it, we can find two subfolders:

* **java** - Here is where the Spark Java code is stored.
* **native** - Here resides part of the native code (C++) from metacache, and the glue logic for JNI.

# Getting started #

## Requirements
In order to build and run **MetaCacheSpark** the following items are needed:

* A Big Data cluster with YARN and HDFS ([Hadoop][2] ecosystem).
* Spark 2.
* Java 8.
* Maven 3.
* A C++ compiler able to build C++14 code.

## Building
The default way to build **MetaCacheSpark** is:

	git clone https://github.com/jmabuin/MetaCacheSpark.git
	cd MetaCacheSpark
	mvn package

This will create the *target* folder, which will contain the *jar* file needed to run **MetaCacheSpark**:

* **MetaCacheSpark-0.4.0.jar** - jar file to launch with Spark.

## Launching
Examples of how to launch **MetaCacheSpark** for building and querying are available at the *script* directory. So far **MetaCacheSpark** only supports *build* and *query* modes.

Available parameters are:

    usage: spark-submit --class com.github.metachachespark.MetaCacheSpark
           MetaCacheSpark-0.4.0.jar [-a <arg>] [-b <arg>] [-c <arg>] [-e] [-g
           <arg>] [-h] [-m <arg>] [-n <arg>] [-o] [-p <arg>] [-q <arg>] [-r]
           [-t <arg>]
    
    MetaCacheSpark performs metagenomics analysis by means of Apache Spark and
    the metacache minhashing algorithm.
    Available parameters:
     -a,--abundance_per <arg>             [QUERY] Indicates if use the
                                          abundance estimation feature and at
                                          which level.
     -b,--buffer_size <arg>               [QUERY] Buffer size to perform query
                                          operations. It indicates the number
                                          of sequences to be query by each
                                          thread.
     -c,--configuration <arg>             [BUILD|QUERY] Configuration file
                                          with parameters to be used inside
                                          MetaCacheSpark.
     -e,--repartition                     [BUILD] Uses Spark repartition
                                          method to repartition sequences
                                          among executors.
     -g,--hits_greater_than <arg>         [QUERY] Gets candidates with more
                                          than specified hits in the
                                          classification maps.
     -h,--help                            Shows documentation
     -m,--mode <arg>                      Operation mode to use with
                                          MetaCacheSpark.
                                          Available options are: build, query,
                                          add, info, annotate.
     -n,--num_threads <arg>               [QUERY] Number of threads per
                                          executor to use in the
                                          classification phase.
     -o,--remove_overpopulated_features   [BUILD] Uses remove overpopulated
                                          features when building.
     -p,--partitions <arg>                [BUILD|QUERY] Number of partitions
                                          to use.
     -q,--query_mode <arg>                [QUERY] Mode selected for query.
                                          Available options are: precise,
                                          threshold, fast, very_fast. Default:
                                          threshold
     -r,--paired_reads                    [QUERY] Use paired reads in the
                                          classification or not.
     -t,--taxonomy <arg>                  [BUILD] Path to the taxonomy to be
                                          used in the HDFS.
    
    Please report issues at josemanuel.abuin@usc.es

### For building
To indicate **MetaCacheSpark** that the build mode is going to be used, the user must indicate the option `-m build`. Important parameters in this mode are:

* **-e**: Indicates if the input reads are distributed among the cluster by using a Spark *repartition* or not. If not, sequences are distributed according their length.
* **-o**: Uses the *remove-overpopulated-features*. As theoretically, in the case of using **MetaCacheSpark** the user is dealing with a lot of data, this option should be used in order to avoid errors during the classification phase.
### For classifying
To indicate **MetaCacheSpark** that the build mode is going to be used, the user must indicate the option `-m query`. Important parameters in this mode are:


[1]: https://github.com/muellan/metacache
[2]: https://hadoop.apache.org/
[3]: https://doi.org/10.1093/bioinformatics/btx520