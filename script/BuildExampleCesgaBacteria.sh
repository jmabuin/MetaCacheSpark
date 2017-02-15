#!/usr/bin/env bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --executor-memory 10G --driver-memory 10G MetaCacheSpark-0.0.2.jar -b -l 128 -t Genomica/genomes/taxonomy/ databasename128.parquet Genomica/genomes/refseq/bacteria/
