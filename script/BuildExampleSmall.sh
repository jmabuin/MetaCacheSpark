#!/usr/bin/env bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 4 --master yarn --deploy-mode cluster --executor-memory 5G --driver-memory 5G MetaCacheSpark-0.3.0.jar -b -l 4 -t metacache/taxonomy/ databasename.parquet metacache/refseq/viral/