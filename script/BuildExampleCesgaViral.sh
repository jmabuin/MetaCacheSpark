#!/usr/bin/env bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 4 --master yarn --deploy-mode cluster --executor-memory 5G --driver-memory 5G MetaCacheSpark-0.0.2.jar -b -l 4 -t Genomica/genomes/taxonomy/ databasename1.parquet Genomica/genomes/refseq/viral/
