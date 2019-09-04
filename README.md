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
### For building


### For classifying

[1]: https://github.com/muellan/metacache
[2]: https://hadoop.apache.org/
[3]: https://doi.org/10.1093/bioinformatics/btx520