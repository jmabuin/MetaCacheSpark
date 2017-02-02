#!/usr/bin/env bash

spark-submit --class com.github.metacachespark.MetaCacheSpark --num-executors 8 --master yarn --deploy-mode cluster --executor-memory 5G --driver-memory 5G MetaCacheSpark-0.0.1.jar -b -l 8 -t Genomica/genomes/taxonomy/ databasename1.parquet Genomica/genomes/refseq/viralsmall/
