#!/usr/bin/env bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --executor-memory 8G --driver-memory 5G MetaCacheSpark-0.2.0.jar -b -o -j -l 32 -t Genomica/genomes/taxonomy/ DatabaseBacteria Genomica/genomes/refseq/bacteria/
