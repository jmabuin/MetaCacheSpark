# What's MetaCacheSpark about? #

**MetaCacheSpark** is a tool that allows to use the metagenomics minhashing algorithm from [**metacache**][1] by means of a Big Data environment by using Apache Spark as engine.

If you use **MetaCacheSpark**, please cite this article:

> Robin Kobus, José M. Abuín, André Müller, Sören Lukas Hellmann, Juan C. Pichel, Tomás F. Pena, Andreas Hildebrandt, Thomas Hankeln and Bertil Schmidt. ["A Big Data Approach to Metagenomics for All-Food-Sequencing"][4]. BMC Bioinformatics, 21, Article number: 102 (2020).

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
Examples of how to launch **MetaCacheSpark** for building and querying are available at the *script* directory. So far **MetaCacheSpark** only supports *build* and *query* modes. Some of the parameters also used in **metacache** are available in the configuration file *src/main/resources/config.properties*. This file can also be passed as argument by using the *-c* option. 

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

* **-q**: Indicates the mode preferred to filter hits during classification. Available modes are, sorted from slower to faster and more precise to less precise:
    * *precise*: All candidates are added to the candidates list.
    * *threshold*: A threshold value is calculated according different parameters. If the number of hits of this candidate is bigger than this threshold value, the candidate is then added to the candidates list.
    * *fast*: Only candidates with more hits than the *hits_greater_than* option are added.
    * *ver_fast*: In this case, only a determined number of candidates (equal or less than *hits_greater_than*) are added to the list.

## Examples
### Build
Execute **MetaCacheSpark** for building with 16 executors. The database will be stored in HDFS at the user home with the name *Database_16*.

    spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000  --num-executors 16 --master yarn --executor-memory 15G --driver-memory 20G MetaCacheSpark-0.4.0.jar -m build -o -p 16 -t /path/to/taxonomy/in/hdfs/ Database_16 /path/to/input/sequences/in/hdfs/

### Query
Execute **MetaCacheSpark** for classification with 16 executors and 4 threads per executor. Results will be stored in HDFS at the user home with the name *Output_File*.

    spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 16 --executor-cores 4 --driver-cores 4 --master yarn --deploy-mode cluster --executor-memory 15G --driver-memory 20G MetaCacheSpark-0.4.0.jar -m query -p 16 -a species -o -b 128000 -n 4 -r Database_16 Output_File /path/to/input/sequences/in/hdfs/
[1]: https://github.com/muellan/metacache
[2]: https://hadoop.apache.org/
[3]: https://doi.org/10.1093/bioinformatics/btx520
[4]: http://dx.doi.org/10.1186%2Fs12859-020-3429-6