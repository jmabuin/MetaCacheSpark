#!/usr/bin/env bash

/home/josemanuel.abuin/Software/spark-2.1.0-bin-without-hadoop/bin/spark-submit --class com.github.metacachespark.MetaCacheSpark --num-executors 4 --master yarn --deploy-mode cluster --executor-memory 5G --driver-memory 5G MetaCacheSpark-0.0.1.jar -b -l 4 -t metacache/taxonomy/ databasename.parquet metacache/refseq/viral/