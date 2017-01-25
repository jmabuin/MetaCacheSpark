#!/usr/bin/env bash

/home/josemanuel.abuin/Software/spark-2.0.1-bin-without-hadoop/bin/spark-submit --class com.github.metacachespark.MetaCacheSpark --num-executors 3 --master yarn --deploy-mode client MetaCacheSpark-0.0.1.jar -b -t metacache/taxonomy/ databasename.parquet metacache/refseq/viral/